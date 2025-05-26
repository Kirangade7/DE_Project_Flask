print("Before import")

from flask import Flask,render_template,request,redirect,url_for

print("After import")

print("Before route")

app = Flask(__name__)

print("Script started")

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/save_data', methods=['GET'])
def save_data():
    regno = request.args.get('regno')
    name = request.args.get('name')
    standard = request.args.get('standard')  # fix the key name to 'standard'
    math = request.args.get('math')
    english = request.args.get('english')
    science = request.args.get('science')
    computer = request.args.get('computer')

    print(f"Received: {regno}, {name}, {standard}, {math}, {english}, {science}, {computer}")

    return redirect(url_for('home'))

if __name__ == "__main__":
    app.run(debug=True)

print("Script ended")

print("Test")
