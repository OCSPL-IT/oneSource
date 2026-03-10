from django.test import TestCase

# Create your tests here.     # To check connection for device
from zk import ZK, const

zk = ZK('192.168.0.30', port=4370, timeout=5)
conn = zk.connect()
print("Connected to device")
conn.disconnect()