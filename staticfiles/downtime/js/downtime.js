$(document).ready(function() {
    // Initialize select2 on eqpt_id
    $('#id_eqpt_id').select2({
        ajax: {
            url: '/search_equipment/',
            dataType: 'json',
            delay: 250,
            data: function(params) {
                return { term: params.term };
            },
            processResults: function(data) {
                // Pass along data.results to Select2
                return { results: data.results };
            }
        },
        minimumInputLength: 1
    });

    // Auto-fill eqpt_name upon selection
    $('#id_eqpt_id').on('select2:select', function(e) {
        let data = e.params.data;
        if (data.eqpt_name) {
            $('#id_eqpt_name').val(data.eqpt_name);
        }
    });
});




$(document).ready(function() {
    // 1) product_name is the FG Name field
    $('#id_product_name').select2({
      ajax: {
        url: '/search_product/',
        dataType: 'json',
        delay: 250,
        data: function(params) {
          return {
            term: params.term,
            search_type: 'fg_name'  // We are searching for FG Names
          };
        },
        processResults: function(data) {
          return { results: data.results };
        }
      },
      minimumInputLength: 1
    });
  });


  $(document).ready(function() {
    // 2) stage_name is the Stage Name field
    $('#id_stage_name').select2({
      ajax: {
        url: '/search_product/',
        dataType: 'json',
        delay: 250,
        data: function(params) {
          return {
            term: params.term,
            search_type: 'stage_name',        // We are searching for Stage Names
            fg_filter: $('#id_product_name').val() // the chosen FG from product_name
          };
        },
        processResults: function(data) {
          return { results: data.results };
        }
      },
      minimumInputLength: 1
    });
  
    // Auto-fill product_code once a stage is selected
    $('#id_stage_name').on('select2:select', function(e) {
      let selected = e.params.data;
      if (selected.product_code) {
        $('#id_product_code').val(selected.product_code);
      }
    });
  });




  $(document).ready(function() {
    // 1) Initialize batch_no field
    $('#id_batch_no').select2({
      ajax: {
        url: '/search_batch/',
        dataType: 'json',
        delay: 250,
        data: function(params) {
          return {
            // pass the selected stage_name from #id_stage_name
            stage_name: $('#id_stage_name').val(),
            term: params.term  // optional partial search for batch no
          };
        },
        processResults: function(data) {
          // data = { results: [...] }
          return { results: data.results };
        }
      },
      minimumInputLength: 0  // can be 1 if you want to force typing
    });
  });




// Idle field ye/No related code
  function toggleNonIdleFields() {
    var idleVal = $('#id_idle').val();
    if (idleVal === "Yes") {
        // Hide the dependent fields
        $('#non_idle_fields').hide();
        // Remove required attribute from all inputs, selects, and textareas in the container
        $('#non_idle_fields').find('input, select, textarea').each(function () {
            $(this).removeAttr('required');
        });
    } else {
        // Show the dependent fields
        $('#non_idle_fields').show();
        // Add required attribute to each field
        $('#non_idle_fields').find('input, select, textarea').each(function () {
            // Optionally, add your own logic to conditionally require some fields
            $(this).attr('required', true);
        });
    }
}

$(document).ready(function () {
    // Run on page load to set correct state
    toggleNonIdleFields();

    // Listen for changes on the idle field
    $('#id_idle').change(function () {
        toggleNonIdleFields();
    });
});




$(document).ready(function() {
  // 1) Listen for changes on eqpt_id or stage_name
  $('#id_eqpt_id, #id_stage_name').on('change', function() {
    let eqptId = $('#id_eqpt_id').val();     // BOMItemCode
    let stageName = $('#id_stage_name').val(); // ItemName

    // Only call if both eqptId and stageName are selected
    if (eqptId && stageName) {
      // Make an AJAX POST request
      $.ajax({
        url: '/get_bom_details/',
        type: 'POST',
        contentType: 'application/json',
        data: JSON.stringify({
          eqpt_id: eqptId,
          stage_name: stageName
        }),
        success: function(response) {
          // response = { bom_qty: <number>, bct: <number> }
          if (response.bom_qty !== undefined) {
            // #id_bom_qty is the "BOM Qty" field
            $('#id_bom_qty').val(response.bom_qty.toFixed(2));
          }
          if (response.bct !== undefined) {
            // #id_bct is the "BCT" field
            $('#id_bct').val(response.bct.toFixed(2));
          }
        },
        error: function(xhr, status, error) {
          console.error('Error fetching BOM details:', error);
          // Optionally reset fields
          $('#id_bom_qty').val('');
          $('#id_bct').val('');
        }
      });
    } else {
      // If eqpt_id or stage_name is not set, reset the fields
      $('#id_bom_qty').val('');
      $('#id_bct').val('');
    }
  });
});


// calculate Totatl duration
function calculateTotalDuration() {
  const container = $('.product-container');
  const startDate = container.find('.start-date').val();
  const startTime = container.find('.start-time').val();
  const endDate = container.find('.end-date').val();
  const endTime = container.find('.end-time').val();
  const durationField = container.find('.total-duration');

  if (startDate && startTime && endDate && endTime) {
    const start = new Date(`${startDate}T${startTime}`);
    const end = new Date(`${endDate}T${endTime}`);
    const diffMs = end - start;
    const diffHours = diffMs / 36e5;
    durationField.val(diffHours.toFixed(2));
    return diffHours;
  } else {
    durationField.val('');
    return 0;
  }
}

function calculateLoss(totalDuration) {
  const container = $('.product-container');
  const bomQty = parseFloat(container.find('.bomqty-value').val()) || 0;
  const bct = parseFloat(container.find('.bct-value').val()) || 0;
  const lossField = container.find('.loss-value');

  let loss = 0;
  if (bct !== 0) {
    loss = (bomQty / bct) * totalDuration;
  }
  lossField.val(loss.toFixed(2));
}

// Recalc duration & loss on date/time changes
$(document).on('change', '.start-date, .start-time, .end-date, .end-time', function() {
  let totalDuration = calculateTotalDuration();
  calculateLoss(totalDuration);
});

// Recalc loss if BOMQty or BCT changes
$(document).on('change', '.bomqty-value, .bct-value', function() {
  let totalDuration = parseFloat($('.product-container').find('.total-duration').val()) || 0;
  calculateLoss(totalDuration);
});
