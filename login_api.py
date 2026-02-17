from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)
CORS(app)

# ---------------- GOOGLE SHEET CONNECT ---------------- #
def connect_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        r"D:\akgvg233saksham\Data\Software\New Creation\gst-backend\API Key\credentials.json",
        scope
    )

    client = gspread.authorize(creds)

    sheet = client.open_by_url(
        "https://docs.google.com/spreadsheets/d/1tt2jKGGNw3sMsmF1mtdhh1fh05XouHN_cBerlihHvFU/edit"
    ).sheet1

    return sheet.get_all_records()


# ---------------- LOGIN VALIDATION ---------------- #
def validate_login(login_id, password):
    records = connect_sheet()

    for row in records:
        if str(row['Login ID']).strip() == login_id and str(row['Password']).strip() == password:
            return True

    return False


# ---------------- ROOT ROUTE ---------------- #
@app.route("/")
def home():
    return "Login API Running"


# ---------------- LOGIN API ---------------- #
@app.route("/login", methods=["POST"])
def login():
    try:
        data = request.json

        if not data:
            return jsonify({"status":"error","message":"No JSON received"}), 400

        login_id = data.get("login_id")
        password = data.get("password")

        if not login_id or not password:
            return jsonify({"status":"error","message":"Missing credentials"}), 400

        if validate_login(login_id, password):
            return jsonify({"status":"success"})

        return jsonify({"status":"invalid"}), 401

    except Exception as e:
        print("LOGIN ERROR:", str(e))
        return jsonify({"status":"error","message":str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)

