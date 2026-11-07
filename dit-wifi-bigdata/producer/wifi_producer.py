#!/usr/bin/env python3
"""
=====================================================================
 wifi_producer.py — KAFKA PRODUCER (Fonte Dadus / Data Source)
=====================================================================
 Simula log husi Access Point (AP) Wi-Fi iha kampus DIT, hanesan log
 ne'ebé Wi-Fi controller loos (UniFi, Aruba, Cisco WLC) haruka liuhosi
 syslog ka REST API. Kada log = evento ida husi kliente (estudante,
 staff, bainaka) ne'ebé liga ba AP ida.

 Modu rua:
   backfill : kria dadus istóriku (ex. 20,000 record, loron 7 liuba)
              → ba batch analytics no Machine Learning
   live     : haruka evento real-time (N evento/segundu) → ba streaming

 Ezemplu:
   python -m producer.wifi_producer --mode backfill --records 20000 --days 7
   python -m producer.wifi_producer --mode live --rate 30
   python -m producer.wifi_producer --mode live --rate 30 --load 1.0 --incident-ap LAB-F2-AP03
=====================================================================
"""
import argparse
import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone

DILI_TZ = timezone(timedelta(hours=9))          # Timor-Leste = UTC+9
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wifi-logs")

# ---------------------------------------------------------------------
# 1. INFRAESTRUTURA KAMPUS (tenke hanesan ho tabela access_points)
#    (kódigu, naran, númeru andar, AP kada andar)
# ---------------------------------------------------------------------
BUILDINGS = [
    ("REK",  "Edifísiu Reitoria",        2, 3),
    ("AULA", "Edifísiu Sala Aula",       3, 4),
    ("LAB",  "Laboratóriu Komputador",   2, 4),
    ("BIB",  "Biblioteka",               2, 3),
    ("KAN",  "Kantina & Área Estudante", 1, 4),
]
_rng_infra = random.Random(42)                  # seed fiksu → kanal AP hanesan beibeik
ACCESS_POINTS = [
    {
        "ap_id": f"{code}-F{f}-AP{a:02d}",
        "building": code,
        "floor": f,
        "ch24": _rng_infra.choice([1, 6, 11]),
        "ch5": _rng_infra.choice([36, 40, 44, 48, 149, 153, 157, 161]),
    }
    for code, _name, floors, aps in BUILDINGS
    for f in range(1, floors + 1)
    for a in range(1, aps + 1)
]

# Padraun uzu kada oras (0–23). 1.0 = oras pik (klase dader).
HOURLY_PROFILE = [0.03, 0.02, 0.02, 0.02, 0.03, 0.05, 0.12, 0.40,
                  0.80, 0.95, 1.00, 0.95, 0.85, 0.90, 0.95, 0.90,
                  0.75, 0.60, 0.55, 0.50, 0.40, 0.25, 0.10, 0.05]

DEVICES = ["smartphone", "laptop", "tablet", "iot"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.07, 0.03]
# Média (log-normal) bytes download kada intervalu USAGE tuir tipu dispozitivu
DEVICE_MU = {"smartphone": 13.0, "laptop": 14.5, "tablet": 13.5, "iot": 9.0}
SSID_BY_ROLE = {"student": "DIT-Student", "staff": "DIT-Staff", "guest": "DIT-Guest"}
EVENT_TYPES = ("CONNECT", "USAGE", "DISCONNECT", "AUTH_FAIL")


def load_factor(ts: datetime, override: float | None = None) -> float:
    """Fator okupasaun kampus (0–1) tuir oras no loron semana."""
    if override is not None:
        return override
    wd = ts.weekday()                                   # 0 = Segunda
    day_mult = 1.0 if wd < 5 else (0.5 if wd == 5 else 0.15)
    return HOURLY_PROFILE[ts.hour] * day_mult


def building_weight(code: str, hour: int) -> float:
    """Edifísiu ne'ebé popular iha oras ida (ex. kantina iha oras han meiudia)."""
    w = {"REK": 0.8, "AULA": 1.5, "LAB": 1.3, "BIB": 1.0, "KAN": 0.7}[code]
    if code == "KAN" and 11 <= hour <= 13:
        w *= 3.0
    if code == "BIB" and 14 <= hour <= 19:
        w *= 1.8
    if code == "AULA" and (hour < 8 or hour > 20):
        w *= 0.3
    return w


# ---------------------------------------------------------------------
# 2. SIMULADOR SESAUN WI-FI
#    Mantein lista sesaun ativu. Kada evento foun:
#      - se uzuáriu ativu < target → barak liu CONNECT
#      - se uzuáriu ativu > target → barak liu DISCONNECT
#      - se lae → USAGE (relatóriu uzu periódiku)
# ---------------------------------------------------------------------
class WifiSimulator:
    def __init__(self, peak_users=900, load_override=None, incident_ap=None, seed=None):
        self.rng = random.Random(seed)
        self.peak_users = peak_users
        self.load_override = load_override
        self.incident_ap = incident_ap           # AP ne'ebé simula problema (demo alerta)
        self.active: dict[str, dict] = {}        # user_id -> sesaun
        self.ap_load = {ap["ap_id"]: 0 for ap in ACCESS_POINTS}
        # ID pseudónimu (la uza naran/NIM loos → proteje privasidade)
        self.users = {
            "student": [f"s_{self.rng.getrandbits(32):08x}" for _ in range(3000)],
            "staff":   [f"t_{self.rng.getrandbits(32):08x}" for _ in range(250)],
        }
        self.user_device: dict[str, str] = {}

    # --- hili AP tuir popularidade edifísiu iha oras ne'e ---
    def _pick_ap(self, hour):
        weights = [building_weight(ap["building"], hour) for ap in ACCESS_POINTS]
        return self.rng.choices(ACCESS_POINTS, weights=weights, k=1)[0]

    def _build_session(self, ts, store=True):
        role = self.rng.choices(["student", "staff", "guest"], [0.82, 0.13, 0.05])[0]
        uid = None
        if role != "guest":
            for _ in range(5):                   # buka uzuáriu ne'ebé seidauk ativu
                cand = self.rng.choice(self.users[role])
                if cand not in self.active:
                    uid = cand
                    break
        if uid is None:                          # bainaka (ka pool nakonu)
            role = "guest"
            uid = f"g_{uuid.uuid4().hex[:8]}"
        device = self.user_device.setdefault(
            uid, self.rng.choices(DEVICES, DEVICE_WEIGHTS)[0])
        p5 = {"laptop": 0.75, "tablet": 0.75, "smartphone": 0.55, "iot": 0.10}[device]
        s = {
            "user_id": uid, "role": role, "device": device,
            "ap": self._pick_ap(ts.hour),
            "band": "5GHz" if self.rng.random() < p5 else "2.4GHz",
            "start": ts,
            "base_rssi": self.rng.gauss(-60, 7),
        }
        if store:
            self.active[uid] = s
            self.ap_load[s["ap"]["ap_id"]] += 1
        return s

    def _close_session(self, uid):
        s = self.active.pop(uid)
        self.ap_load[s["ap"]["ap_id"]] -= 1

    def _make_event(self, ts, s, etype):
        """Konverte sesaun → log JSON (hanesan format controller Wi-Fi)."""
        ap = s["ap"]
        load = self.ap_load[ap["ap_id"]]
        rssi = s["base_rssi"] + self.rng.gauss(0, 3)
        # Latency no packet loss aumenta bainhira sinal fraku ka AP nakonu
        latency = 4 + max(0, -rssi - 55) * 0.8 + load * 1.0 + self.rng.expovariate(1 / 6)
        loss = max(0.0, (-rssi - 70) * 0.25 + load * 0.04 + self.rng.gauss(0, 0.3))
        if ap["ap_id"] == self.incident_ap:     # simula AP ne'ebé aat
            rssi -= 18
            latency *= 4
            loss += 6
        rssi = int(max(-95, min(-30, rssi)))

        if etype == "USAGE":
            down = int(self.rng.lognormvariate(DEVICE_MU[s["device"]], 1.0))
            up = int(down * self.rng.uniform(0.05, 0.35))
        elif etype in ("CONNECT", "DISCONNECT"):
            down, up = self.rng.randint(2_000, 50_000), self.rng.randint(1_000, 20_000)
        else:                                    # AUTH_FAIL → la iha tráfiku
            down = up = 0

        return {
            "event_id": str(uuid.uuid4()),
            "event_time": ts.isoformat(timespec="milliseconds"),
            "event_type": etype,
            "ap_id": ap["ap_id"],
            "building": ap["building"],
            "floor": ap["floor"],
            "ssid": SSID_BY_ROLE[s["role"]],
            "band": s["band"],
            "channel": ap["ch5"] if s["band"] == "5GHz" else ap["ch24"],
            "user_id": s["user_id"],
            "user_role": s["role"],
            "device_type": s["device"],
            "rssi_dbm": rssi,
            "bytes_up": up,
            "bytes_down": down,
            "session_duration_s": int((ts - s["start"]).total_seconds()),
            "latency_ms": round(latency, 2),
            "packet_loss_pct": round(min(loss, 100.0), 2),
        }

    def next_event(self, ts: datetime) -> dict:
        target = max(5, int(self.peak_users * load_factor(ts, self.load_override)))
        n = len(self.active)

        if self.rng.random() < 0.03:             # 3% autentikasaun falla
            return self._make_event(ts, self._build_session(ts, store=False), "AUTH_FAIL")

        p_connect = 0.45 if n < target else 0.04
        p_disc = 0.45 if n > target else 0.04
        r = self.rng.random()

        if n == 0 or r < p_connect:
            return self._make_event(ts, self._build_session(ts), "CONNECT")
        uid = self.rng.choice(list(self.active))
        s = self.active[uid]
        if r < p_connect + p_disc:
            evt = self._make_event(ts, s, "DISCONNECT")
            self._close_session(uid)
            return evt
        return self._make_event(ts, s, "USAGE")

    def corrupt(self, evt: dict, last: dict | None) -> list[dict]:
        """Hatama dadus 'foer' ho hakarak → atu testa data quality pipeline."""
        kind = self.rng.choice(["missing_ap", "bad_rssi", "negative_bytes", "bad_time", "duplicate"])
        e = dict(evt)
        if kind == "missing_ap":
            e["ap_id"] = None
        elif kind == "bad_rssi":
            e["rssi_dbm"] = self.rng.choice([15, -150])
        elif kind == "negative_bytes":
            e["bytes_down"] = -abs(e["bytes_down"] or 1000)
        elif kind == "bad_time":
            e["event_time"] = "31/13/2026 25:61"
        elif kind == "duplicate" and last is not None:
            return [evt, dict(last)]             # haruka fali evento anterior (event_id hanesan)
        return [e]


# ---------------------------------------------------------------------
# 3. KAFKA: kria topic no haruka mensajen
# ---------------------------------------------------------------------
def ensure_topic(partitions=3):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if TOPIC in admin.list_topics(timeout=10).topics:
        return
    fut = admin.create_topics([NewTopic(TOPIC, num_partitions=partitions, replication_factor=1)])
    try:
        fut[TOPIC].result()
        print(f"[producer] Topic '{TOPIC}' kria ho {partitions} partisaun")
    except Exception as exc:                     # topic kria ona husi prosesu seluk
        print(f"[producer] Topic: {exc}")


def build_producer():
    from confluent_kafka import Producer
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",                # hein konfirmasaun broker → la lakon dadus
        "enable.idempotence": True,   # retry la kria duplikadu iha Kafka
        "linger.ms": 20,              # grupu mensajen kada 20 ms → throughput aas
        "batch.num.messages": 1000,
        "compression.type": "lz4",    # kompresaun → uza network menus
    })


def main():
    ap = argparse.ArgumentParser(description="Simulador log Wi-Fi DIT → Kafka")
    ap.add_argument("--mode", choices=["backfill", "live"], default="live")
    ap.add_argument("--records", type=int, default=20000, help="backfill: total record")
    ap.add_argument("--days", type=float, default=7, help="backfill: loron istóriku")
    ap.add_argument("--rate", type=float, default=30, help="live: evento kada segundu")
    ap.add_argument("--max-events", type=int, default=0, help="live: para depois N (0 = la para)")
    ap.add_argument("--peak-users", type=int, default=900)
    ap.add_argument("--load", type=float, default=None, help="fiksa fator okupasaun (0–1) ba demo")
    ap.add_argument("--dirty-rate", type=float, default=0.02, help="proporsaun dadus foer")
    ap.add_argument("--incident-ap", default=None, help="AP ne'ebé simula problema")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    ensure_topic()
    producer = build_producer()
    sim = WifiSimulator(args.peak_users, args.load, args.incident_ap, args.seed)
    stats = {"sent": 0, "failed": 0}
    running = True

    def on_delivery(err, _msg):
        if err is not None:
            stats["failed"] += 1

    def send(evt):
        payload = json.dumps(evt).encode("utf-8")
        key = (evt.get("ap_id") or "unknown").encode()   # key = AP → orden kada AP mantein
        while True:
            try:
                producer.produce(TOPIC, key=key, value=payload, on_delivery=on_delivery)
                break
            except BufferError:                  # buffer lokal nakonu → hein uitoan
                producer.poll(0.5)
        producer.poll(0)
        stats["sent"] += 1

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    last = None
    rng = random.Random(args.seed)

    def emit(ts):
        nonlocal last
        evt = sim.next_event(ts)
        batch = sim.corrupt(evt, last) if rng.random() < args.dirty_rate else [evt]
        for e in batch:
            send(e)
        last = evt

    if args.mode == "backfill":
        # Distribui timestamp tuir padraun oras (barak liu iha oras klase)
        end = datetime.now(DILI_TZ)
        start = end - timedelta(days=args.days)
        minutes = int(args.days * 1440)
        weights = [load_factor(start + timedelta(minutes=m)) + 0.01 for m in range(minutes)]
        slots = sorted(rng.choices(range(minutes), weights=weights, k=args.records))
        print(f"[producer] BACKFILL {args.records:,} record: {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}")
        for i, m in enumerate(slots, 1):
            if not running:
                break
            emit(start + timedelta(minutes=m, seconds=rng.random() * 60))
            if i % 5000 == 0:
                print(f"  ... {i:,} haruka ona")
    else:
        print(f"[producer] LIVE {args.rate}/s → topic '{TOPIC}' @ {BOOTSTRAP}  (Ctrl+C atu para)")
        interval = 1.0 / args.rate
        next_t = t0 = time.time()
        last_report, last_sent = t0, 0
        while running and (args.max_events == 0 or stats["sent"] < args.max_events):
            emit(datetime.now(DILI_TZ))
            next_t += interval
            delay = next_t - time.time()
            if delay > 0:
                time.sleep(delay)
            now = time.time()
            if now - last_report >= 5:
                rate = (stats["sent"] - last_sent) / (now - last_report)
                print(f"  sent={stats['sent']:,}  rate={rate:.1f}/s  sesaun_ativu={len(sim.active)}  "
                      f"falla={stats['failed']}")
                last_report, last_sent = now, stats["sent"]

    producer.flush(30)
    print(f"[producer] REMATA. Total haruka={stats['sent']:,}, falla={stats['failed']}")


if __name__ == "__main__":
    main()
