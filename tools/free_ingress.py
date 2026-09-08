"""Drop every LiveKit ingress that is not publishing right now and clear its stream row.
Run against prod with:  railway run .venv/bin/python tools/free_ingress.py"""
import livekit_api as lk
from indexer import db

c = db.connect_web()
for i in lk.list_ingress():
    st = (i.get("state") or {}).get("status")
    if st != "ENDPOINT_PUBLISHING":
        lk.delete_ingress(i["ingress_id"])
        c.execute("UPDATE streams SET ingress_id='', rtmps_url='', stream_key='' WHERE ingress_id=?", (i["ingress_id"],)); c.commit()
        print("freed", i["ingress_id"], i.get("room_name"), st)
print("left:", [(x["ingress_id"], (x.get("state") or {}).get("status")) for x in lk.list_ingress()])
