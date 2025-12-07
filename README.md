<div align="center">

# Make A Sample! <p style="font-size:20px">Framework for Creating Balanced Language Samples<p>

</div>

## Who are we?

We are a team of linguistics students from HSE University passionate about language and technology. This website is part of our educational project in the Bachelor's program in Fundamental and Computational Linguistics. [Gleb Bubnov](https://github.com/Interprole) has worked on the backend, [Veronika Tsareva](https://github.com/veronikatsareva) has worked on the frontend, with guidance from our supervisors, [Anton Buzanov](https://github.com/vantral) and [Maxim Bazhukov](https://github.com/bamaxi).

## What does our framework do?

This site is built for typologists who want flexible, data-driven tools. We have united data from [Glottolog](https://glottolog.org/), [Grambank](https://grambank.clld.org/) and [WALS Online](https://wals.info/) in order to let linguists create their own custom samples.

## Usage

Our framework runs on a [server](http://104.207.134.107:5002/). However, it is possible to run the website locally.

In order to do that, you need to 

1) clone the whole repository

```bash
git clone https://github.com/Interprole/Sampling.git
```

2) install all dependencies from [`requirements.txt`](requirements.txt)

```bash
pip install -r requirements.txt
```

3) run [`app.py`](app.py)

```bash
python app.py
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

## License

[GNU GENERAL PUBLIC LICENSE](https://choosealicense.com/licenses/gpl-3.0/)