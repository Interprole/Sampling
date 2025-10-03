$(document).ready(function() {
    $('.js-example-basic-multiple').select2({
        closeOnSelect: false,
    });

    $('.doc-lang-js').select2({
        closeOnSelect: false,
        ajax: {
            url: 'static/documentLanguages.json',
            dataType: 'json',
        }
    });

    $('.include-lang-js, .exclude-lang-js').select2({
        closeOnSelect: false,
        ajax: {
            url: 'static/languages.json',
            dataType: 'json'
        }
    });

    $('.grambank-js').select2({
        closeOnSelect: false,
        ajax: {
            url: 'static/grambankFeatures.json',
            dataType: 'json'
        }
    });

});