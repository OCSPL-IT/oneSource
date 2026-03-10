
document.addEventListener('DOMContentLoaded', function () {
    const dropdownToggles = document.querySelectorAll('.dropdown-toggle');

    dropdownToggles.forEach(button => {
        button.addEventListener('mouseover', function () {
            this.parentNode.querySelector('.dropdown-menu').style.display = 'block';
        });

        button.parentNode.addEventListener('mouseleave', function () {
            this.querySelector('.dropdown-menu').style.display = 'none';
        });
    });
});
