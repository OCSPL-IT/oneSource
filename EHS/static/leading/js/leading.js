function calculateRiskFactor() {
    const severity = parseInt(document.getElementById("severity").value);
    const likelihood = parseInt(document.getElementById("likelihood").value);
    const risk_factorField = document.getElementById("risk_factor");

    if (!isNaN(severity) && !isNaN(likelihood)) {
        const risk_factorValue = severity * likelihood;
        if (risk_factorValue <= 2) {
            risk_factorField.value = `${risk_factorValue} - Low`;
            risk_factorField.style.backgroundColor = "#A7F3D0";
        } else if (risk_factorValue <= 6) {
            risk_factorField.value = `${risk_factorValue} - Medium`;
            risk_factorField.style.backgroundColor = "#FEF08A";
        } else {
            risk_factorField.value = `${risk_factorValue} - High`;
            risk_factorField.style.backgroundColor = "#FCA5A5";
        }
    } else {
        risk_factorField.value = "";
        risk_factorField.style.backgroundColor = "";
    }
}