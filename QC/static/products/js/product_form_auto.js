// products/static/products/js/product_form_auto.js

/**
 * Copies the `data-code` and `data-type` attributes
 * from the currently selected <option> into the two
 * read-only fields: #id_code and #id_item_type.
 */
function updateCodeAndType(selectElem) {
  var selectedOption = selectElem.options[ selectElem.selectedIndex ];
  if (!selectedOption) {
    document.getElementById('id_code').value      = '';
    document.getElementById('id_item_type').value = '';
    return;
  }
  var codeVal = selectedOption.getAttribute('data-code') || '';
  var typeVal = selectedOption.getAttribute('data-type') || '';
  document.getElementById('id_code').value      = codeVal;
  document.getElementById('id_item_type').value = typeVal;
}

document.addEventListener('DOMContentLoaded', function() {
  // Find the ERP dropdown
  var sel = document.getElementById('id_item_choice');
  if (!sel) {
    return;
  }

  // 1) If the dropdown already has a value (e.g. on “edit” mode),
  //    populate the Code + Item Type fields once on page load.
  if (sel.value) {
    updateCodeAndType(sel);
  }

  // 2) Attach a 'change' listener so that whenever the user picks
  //    a different ERP item, we re-populate the two read-only fields.
  sel.addEventListener('change', function() {
    updateCodeAndType(this);
  });
});
