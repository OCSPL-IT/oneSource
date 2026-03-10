document.addEventListener('DOMContentLoaded', function () {
    // Get DOM elements
    const dateField = document.getElementById('date');
    const productionField = document.getElementById('contract_labour_production');
    const othersField = document.getElementById('contract_labour_others');
    const totalLaboursField = document.getElementById('total_supplied_casual_labours');
    const hrField = document.getElementById('hr');
    const totalHRField = document.getElementById('total_no_of_hr');

    // Auto-fill today's date if the field is empty
    if (dateField && !dateField.value) {
        const today = new Date().toISOString().split('T')[0];
        dateField.value = today;
    }

    // Function to calculate total values
    function calculateFields() {
        // Parse field values as integers or default to 0
        const production = parseInt(productionField.value) || 0;
        const others = parseInt(othersField.value) || 0;
        const hr = parseInt(hrField.value) || 0;

        // Calculate total supplied casual labours
        const totalLabours = production + others;
        totalLaboursField.value = totalLabours;

        // Calculate total number of HR
        const totalHR = totalLabours * hr;
        totalHRField.value = totalHR;
    }

    // Attach event listeners to update calculations dynamically
    if (productionField && othersField && hrField) {
        productionField.addEventListener('input', calculateFields);
        othersField.addEventListener('input', calculateFields);
        hrField.addEventListener('input', calculateFields);
    }

    // Initial calculation in case values are pre-filled
    calculateFields();
});
