document.addEventListener("DOMContentLoaded", function() {
    const reportingDateInput = document.getElementById("id_reporting_date");
    const unloadingDateInput = document.getElementById("id_unloading_date");
    const unloadingDaysInput = document.getElementById("unloading_days");
    // const statusInput = document.getElementById("status");

    function updateUnloadingDays() {
        const reportingDate = new Date(reportingDateInput.value);
        const unloadingDate = new Date(unloadingDateInput.value);
        
        if (!isNaN(reportingDate) && !isNaN(unloadingDate)) {
            const diffTime = unloadingDate - reportingDate;
            const diffDays = diffTime / (1000 * 60 * 60 * 24);
            unloadingDaysInput.value = diffDays;
            statusInput.value = diffDays >= 0 ? "Unloaded" : "Pending";
        } else {
            unloadingDaysInput.value = "";
            // statusInput.value = "";
        }
    }

    reportingDateInput.addEventListener("change", updateUnloadingDays);
    unloadingDateInput.addEventListener("change", updateUnloadingDays);
});



$(document).ready(function() {
    $('#id_name_of_supplier').select2({
        ajax: {
            url: '/search_supplier/',  // Ensure this URL is correct
            dataType: 'json',
            delay: 250,
            data: function (params) {
                return { term: params.term };
            },
            processResults: function (data) {
                return { results: data };
            }
        },
        minimumInputLength: 1
    });

    $('#id_material').select2({
        ajax: {
            url: '/search_item/',  // Ensure this URL is correct
            dataType: 'json',
            delay: 250,
            data: function (params) {
                return { term: params.term };
            },
            processResults: function (data) {
                return { results: data };
            }
        },
        minimumInputLength: 1
    });
});



