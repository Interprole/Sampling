async function getJsonData(url) {
    const response = await fetch(url);
    const javascriptObject = await response.json();
    return javascriptObject.results;
};

// Filtering results from api-endpoint
function processResultsWithChildren(data, params) {
    const searchTerm = (params.term || '').toLowerCase().trim();
    if (searchTerm === '') {
        const results = data.map(feature => ({
            text: `${feature.code}_${feature.name}`,
            children: feature.values.map(value => ({
                id: value.code,
                text: `${feature.code}_${feature.name} | ${value.name}`
            }))
        }));
        return {
            results: results,
            pagination: {
                more: false
            }
        };
    }
    const filteredResults = [];
    data.forEach(feature => {
        const filteredChildren = feature.values.filter(value =>
            `${feature.code}_${feature.name} | ${value.name}`.toLowerCase().includes(searchTerm)
        );
        if (filteredChildren.length > 0) {
            const mappedChildren = filteredChildren.map(value => ({
                id: value.code,
                text: `${feature.code}_${feature.name} | ${value.name}`
            }));
            filteredResults.push({
                text: `${feature.code}_${feature.name}`,
                children: mappedChildren
            });
        }
    });
    return {
        results: filteredResults,
        pagination: {
            more: false
        }
    };
};

$(document).ready(function() {
    // Tooltips
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]')
    const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(
        tooltipTriggerEl, {
            container: 'body'
        }));
    
    // Setting options for Select2 from api-endpoint json
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
    $('.grambank-js').select2({
        width: '100%',
        theme: "classic",
        closeOnSelect: false,
        ajax: {
            url: '/api/features/grambank',
            dataType: "json",
            data: function(params) {
                return {
                    q: params.term,
                    page: params.page
                };
            },
            processResults: processResultsWithChildren
        }
    });
    $('.wals-js').select2({
        width: '100%',
        theme: "classic",
        closeOnSelect: false,
        data: function(params) {
            return {
                q: params.term,
                page: params.page
            };
        },
        ajax: {
            url: '/api/features/wals',
            dataType: "json",
            processResults: processResultsWithChildren
        }
    });
});