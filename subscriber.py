from flask import Flask, request, jsonify
import requests, json

app = Flask(__name__)

ORION_LD_HOST = "http://192.168.1.23:1026"
SUBS_URL = f"{ORION_LD_HOST}/ngsi-ld/v1/subscriptions"
CALLBACK = "http://192.168.1.27:8000/notify"
SUB_ID = "urn:ngsi-ld:Subscription:KitchenUpdates"

payload = {
    "id": SUB_ID,
    "type": "Subscription",
    "entities": [{"id": "urn:ngsi-ld:Kitchen:Kitchen", "type": "Kitchen"}],
    "watchedAttributes": ["temperature", "humidity", "brightness"],
    "notification": {
        "attributes": ["temperature", "humidity", "brightness"],
        "format": "normalized",
        "endpoint": {"uri": CALLBACK, "accept": "application/ld+json"}
    },
    "@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
}

def setup_subscription():
    requests.delete(f"{SUBS_URL}/{SUB_ID}", timeout=5)
    r = requests.post(SUBS_URL, json=payload, headers={"Content-Type":"application/ld+json"}, timeout=5)
    print("Subscription:", r.status_code, r.text)

@app.route("/notify", methods=["POST"])
def notify():
    data = request.get_json(force=True)
    print("📥 Notification received:", json.dumps(data, indent=2))
    return jsonify({"status": "received"}), 200

if __name__ == "__main__":
    setup_subscription()
    app.run(host="0.0.0.0", port=8000)