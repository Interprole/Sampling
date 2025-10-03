from flask import Flask, redirect, url_for
from flask import render_template
from flask import request

app = Flask(__name__)

@app.route("/")
def main():
    return render_template("main.html")


@app.route("/sample", methods=['POST'])
def sample():
    if request.method == "POST":
        title = request.form['name']
        algorithm = request.form['sampling-algorithm']
        size = request.form['sample-size']
        macroareas = request.form.getlist('macroareas[]')
        docLang = request.form.getlist('docLanguages[]')
        rank = request.form['ranking']
        includeLang = request.form.getlist('include[]')
        excludeLang = request.form.getlist('exclude[]')
        gramFeature = request.form.getlist('grambank[]')
    
    return render_template("sample.html", sample=sample)


@app.route("/manual")
def manual():
    return render_template("manual.html")


@app.route("/about")
def about():
    return render_template("about.html")


if __name__ == "__main__":
    app.run(debug=True)