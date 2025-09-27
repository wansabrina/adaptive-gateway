import time, socket, statistics, requests, random, psutil, grpc, threading
from concurrent import futures
from flask import Flask, request, jsonify
import echo_pb2, echo_pb2_grpc

class EchoServicer(echo_pb2_grpc.EchoServicer):
    def Send(self, request, context):
        return echo_pb2.EchoReply(message=request.message)

def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    echo_pb2_grpc.add_EchoServicer_to_server(EchoServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    return server

app = Flask(__name__)

@app.route("/echo", methods=["POST"])
def echo_rest():
    data = request.json.get("payload", "")
    return jsonify({"message": data})

def start_rest_server():
    def run():
        app.run(host="0.0.0.0", port=5000)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

def ping_latency(host="8.8.8.8", count=3):
    latencies = []
    for _ in range(count):
        start = time.time()
        try:
            socket.create_connection((host, 53), timeout=1)
            latencies.append((time.time() - start) * 1000)
        except Exception:
            latencies.append(999)
    return statistics.mean(latencies)

def measure_bandwidth():
    url = "https://speed.cloudflare.com/__down?bytes=1000000"
    start = time.time()
    resp = requests.get(url, headers={"Connection": "close"})
    elapsed = time.time() - start
    download_speed = (len(resp.content) * 8 / 1_000_000) / elapsed
    data = b"x" * 500_000
    start = time.time()
    requests.post("https://httpbin.org/post", data=data, headers={"Connection": "close"})
    elapsed = time.time() - start
    upload_speed = (len(data) * 8 / 1_000_000) / elapsed
    return round(download_speed, 2), round(upload_speed, 2)

def cpu_usage():
    return psutil.cpu_percent(interval=0)

def send_rest(data, timeout=15):
    start = time.time()
    try:
        resp = requests.post(
            "http://127.0.0.1:5000/echo",
            json={"payload": data},
            timeout=timeout
        )
        elapsed = (time.time() - start) * 1000
        return resp.status_code, len(resp.content), elapsed, "REST"
    except requests.exceptions.RequestException:
        elapsed = (time.time() - start) * 1000
        return 408, 0, elapsed, "REST (timeout)"

def send_grpc(data):
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = echo_pb2_grpc.EchoStub(channel)
        start = time.time()
        response = stub.Send(echo_pb2.EchoRequest(message=data))
        elapsed = (time.time() - start) * 1000
        return 200, len(response.message.encode()), elapsed, "gRPC"

def choose_protocol_level1(size):
    if size < 1000:
        return "REST", "payload <1KB"
    else:
        return "gRPC", "payload >=1KB"

def choose_protocol_level2(size, latency, download_bw):
    if latency > 200 or download_bw < 5:
        return "REST", "network slow"
    elif size < 1000:
        return "REST", "small payload"
    else:
        return "gRPC", "large payload"

def choose_protocol_level3(size, latency, download_bw, cpu, response_time=None):
    if latency > 200:
        return "REST", "forced REST high latency"
    if download_bw < 5:
        return "REST", "forced REST low bandwidth"
    if size < 1000:
        return "REST", "forced REST small payload"
    score_rest, score_grpc = 0, 0
    if latency > 100: score_rest += 2
    else: score_grpc += 2
    if download_bw < 10: score_rest += 2
    else: score_grpc += 2
    if size >= 1000: score_grpc += 2
    else: score_rest += 2
    if cpu > 70: score_rest += 1
    else: score_grpc += 1
    if response_time and response_time > 500:
        score_rest += 2
    elif response_time and response_time < 200:
        score_grpc += 1
    return ("REST", f"score REST={score_rest}, gRPC={score_grpc}") if score_rest >= score_grpc else ("gRPC", f"score REST={score_rest}, gRPC={score_grpc}")

def get_ground_truth(size, latency, download_bw):
    if size < 1000:
        return "REST"
    elif latency > 200 or download_bw < 5:
        return "REST"
    else:
        return "gRPC"

def run_with_rtt(proto, data):
    if proto == "REST":
        _, _, rtt, _ = send_rest(data)
    else:
        _, _, rtt, _ = send_grpc(data)
    return rtt

def run_batch(batch_name, latency, download_bw, upload_bw, cpu, total=5):
    print(f"\n== {batch_name} ==")
    print(f"Latency={latency}ms | Download={download_bw}Mbps | Upload={upload_bw}Mbps | CPU={cpu}%\n")
    acc1, acc2, acc3 = 0, 0, 0
    for i in range(total):
        size = random.choice([500, 5000, 50000, 500000, 2000000])
        data = "X" * size
        gt = get_ground_truth(size, latency, download_bw)

        proto1, reason1 = choose_protocol_level1(size)
        rtt1 = run_with_rtt(proto1, data)
        if proto1 == gt: acc1 += 1

        proto2, reason2 = choose_protocol_level2(size, latency, download_bw)
        rtt2 = run_with_rtt(proto2, data)
        if proto2 == gt: acc2 += 1

        proto3, reason3 = choose_protocol_level3(size, latency, download_bw, cpu)
        rtt3 = run_with_rtt(proto3, data)
        if proto3 == gt: acc3 += 1

        print(f"Test {i+1}: size={size}B | GT={gt}")
        print(f"  [L1] {proto1} | reason={reason1} | correct={proto1==gt} | RTT={rtt1:.2f}ms")
        print(f"  [L2] {proto2} | reason={reason2} | correct={proto2==gt} | RTT={rtt2:.2f}ms")
        print(f"  [L3] {proto3} | reason={reason3} | correct={proto3==gt} | RTT={rtt3:.2f}ms\n")

    print("== Evaluation Summary ==")
    print(f"Level 1 Accuracy: {acc1}/{total} = {acc1/total*100:.2f}%")
    print(f"Level 2 Accuracy: {acc2}/{total} = {acc2/total*100:.2f}%")
    print(f"Level 3 Accuracy: {acc3}/{total} = {acc3/total*100:.2f}%")

def main():
    server = start_grpc_server()
    start_rest_server()
    try:
        run_batch("Batch 1 (Normal)", latency=50, download_bw=15, upload_bw=2, cpu=20)
        run_batch("Batch 2 (Low Bandwidth)", latency=50, download_bw=3, upload_bw=0.8, cpu=30)
        run_batch("Batch 3 (High Latency)", latency=250, download_bw=12, upload_bw=2, cpu=40)
        run_batch("Batch 4 (High CPU)", latency=60, download_bw=14, upload_bw=1.5, cpu=85)
        run_batch("Batch 5 (Aggressive L3 Case)", latency=80, download_bw=4, upload_bw=2, cpu=10)
    finally:
        server.stop(0)

if __name__ == "__main__":
    main()
