from datetime import time
from .models import *
from .models import Shift

# Create your views here.

def determine_shift(punch_datetime):
    allowed_shifts = [
        {"name": "Lunch F", "start": time(11, 30), "end": time(13, 00)},
        {"name": "Lunch G", "start": time(13, 0), "end": time(14, 30)},
        {"name": "Dinner", "start": time(19, 30), "end": time(20, 30)},
        {"name": "Evening Dinner", "start": time(20, 30), "end": time(21, 30)},
        {"name": "Morning Tea", "start": time(8, 30), "end": time(10, 00)},
        {"name": "Evening Tea", "start": time(16, 00), "end": time(17, 30)},
        {"name": "Night Tea", "start": time(3, 30), "end": time(4, 00)},
    ]
    
    punch_time = punch_datetime.time()

    for period in allowed_shifts:
        if period["start"] <= punch_time <= period["end"]:
            shift, _ = Shift.objects.get_or_create(name=period["name"])
            if shift.start_time != period["start"] or shift.end_time != period["end"]:
                shift.start_time = period["start"]
                shift.end_time = period["end"]
                shift.save()
            return shift

    return None




''''==========   punching machine related code  ==============================='''

# utils.py
from zk import ZK
import pandas as pd


def get_next_uid(ip_address="192.168.0.30", port=4370):
    from zk import ZK
    zk = ZK(ip_address, port=port, timeout=5)
    conn = None
    try:
        conn = zk.connect()
        users = conn.get_users()
        if not users:
            return 1  # Device is empty
        max_uid = max([u.uid for u in users if u.uid is not None])
        return max_uid + 1
    finally:
        if conn:
            conn.disconnect()



def add_user_to_device(
    user_id,
    name,
    card=None,
    ip_address="192.168.0.30",
    port=4370,
):
    # print(f"[UTIL] Params - user_id: {user_id}, name: {name}, card: {card}")
    
    try:
        uid = get_next_uid(ip_address, port)
        # print(f"[UTIL] Auto-increment UID to use: {uid}")
        zk = ZK(ip_address, port=port, timeout=5)
        conn = zk.connect()
        conn.disable_device()
        try:
            # print("[UTIL] Setting user on device...")
            conn.set_user(
                uid=uid,
                name=name,
                user_id=str(user_id),
                **({"card": int(card)} if card else {})  # <- only send card if provided
            )
            # print("[UTIL] User set successfully!")
        finally:
            conn.enable_device()
            conn.disconnect()
        return True, f"User {name} added to device with USER_ID {user_id}."
    except Exception as e:
        # print("[UTIL] Exception occurred:", repr(e))
        return False, str(e)



def delete_user_from_device(user_id, ip_address="192.168.0.30", port=4370):
    from zk import ZK
    try:
        zk = ZK(ip_address, port=port, timeout=5)
        conn = zk.connect()
        conn.disable_device()
        conn.delete_user(user_id=str(user_id))  # user_id as string
        conn.enable_device()
        conn.disconnect()
        return True, f"User {user_id} deleted from device {ip_address}"
    except Exception as e:
        # print("[UTIL] Error deleting user:", e)
        return False, str(e)




def get_all_users_from_device(ip_address="192.168.0.30", port=4370):
    try:
        zk = ZK(ip_address, port=port, timeout=5)
        conn = zk.connect()
        users = conn.get_users()
        # Example user fields: uid, user_id, name, privilege, card, group_id, user_group, password
        user_data = []
        for user in users:
            user_data.append({
                'UID': user.uid,
                'User ID': user.user_id,
                'Name': user.name,
                'Privilege': user.privilege,
                'Card': user.card,
                'Group ID': getattr(user, 'group_id', ''),
                'User Group': getattr(user, 'user_group', ''),
            })
        conn.disconnect()
        df = pd.DataFrame(user_data)
        return df
    except Exception as e:
        # print("[UTIL] Error fetching users:", e)
        return None