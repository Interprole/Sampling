from flask import Flask, redirect, url_for
from flask import render_template
from flask import request

app = Flask(__name__)

@app.route("/")
def main():
    return render_template("main.html")


@app.route("/manual")
def manual():
    return render_template("manual.html")


@app.route("/about")
def about():
    return render_template("about.html")


if __name__ == "__main__":
    app.run(debug=True)