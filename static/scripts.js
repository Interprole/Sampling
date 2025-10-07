async function getJsonData(url) {
    const response = await fetch(url);
    const javascriptObject = await response.json();
    return javascriptObject.results;
};

$(document).ready(function() {
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]')
    const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl, {
        container: 'body'
    }));

    $('.js-example-basic-single, .js-example-basic-multiple').select2({
        width: '100%',
        theme: "classic",
        closeOnSelect: false,
    });

    getJsonData("static/documentLanguages.json")
        .then(data =>
            $('.doc-lang-js').select2({
                width: '100%',
                theme: "classic",
                data: data,
                closeOnSelect: false
            })
        );

    getJsonData("static/languages.json")
        .then(data =>
            $('.include-lang-js, .exclude-lang-js').select2({
                width: '100%',
                theme: "classic",
                data: data,
                closeOnSelect: false
            })
        );

    getJsonData("static/grambankFeatures.json")
        .then(data =>
            $('.grambank-js').select2({
                width: '100%',
                theme: "classic",
                data: data,
                closeOnSelect: false
            })
        );

    getJsonData("static/walsFeatures.json")
        .then(data =>
            $('.wals-js').select2({
                width: '100%',
                theme: "classic",
                data: data,
                closeOnSelect: false
            })
        );
});