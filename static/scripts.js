$(document).ready(function() {
    $('.js-example-basic-single, .js-example-basic-multiple').select2({
        width: '100%',
        theme: "classic",
        closeOnSelect: false,
    });

    $('.doc-lang-js').select2({
        width: '100%',
        theme: "classic",
        closeOnSelect: false,
        ajax: {
            url: 'static/documentLanguages.json',
            dataType: 'json',
        }
    });

    $('.include-lang-js, .exclude-lang-js').select2({
        width: '100%',
        theme: "classic",
        closeOnSelect: false,
        ajax: {
            url: 'static/languages.json',
            dataType: 'json'
        }
    });

    $('.grambank-js').select2({
        width: '100%',
        theme: "classic",
        closeOnSelect: false,
        ajax: {
            url: 'static/grambankFeatures.json',
            dataType: 'json',
        }
    });

    $('.wals-js').select2({
        width: '100%',
        theme: "classic",
        closeOnSelect: false,
        ajax: {
            url: 'static/walsFeatures.json',
            dataType: 'json'
        }
    });
});