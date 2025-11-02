import zmq

ctx = zmq.Context()
sub = ctx.socket(zmq.SUB)
sub.connect("tcp://18.195.228.143:6500")  # <-- new public IP
sub.setsockopt_string(zmq.SUBSCRIBE, "")
print("✅ Connected to remote HFT feed at 18.195.228.143:6500")

while True:
    topic, data = sub.recv_multipart()
    print(topic.decode(), data.decode())
