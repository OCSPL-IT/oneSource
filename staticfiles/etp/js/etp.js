$(function(){

    // ── 1) Select2 inits ───────────────────────────────────────────────────────
    $('#id_product_name').select2({
      ajax:{ url:'/get_products/', dataType:'json', delay:250,
        data: params=>({ term:params.term }),
        processResults: data=>({ results:data.results })
      },
      minimumInputLength:1
    });
    $('#id_stage_name').select2({
      ajax:{ url:'/get_stage_names/', dataType:'json', delay:250,
        data: params=>({
          term:params.term,
          product_name:$('#id_product_name').val()
        }),
        processResults: data=>({ results:data.results })
      },
      minimumInputLength:1
    });
    $('#id_batch_no').select2({
      ajax:{ url:'/get_batch_nos/', dataType:'json', delay:250,
        data: params=>({
          term:params.term,
          product_name:$('#id_product_name').val(),
          stage_name:$('#id_stage_name').val()
        }),
        processResults: data=>({ results:data.results })
      },
      minimumInputLength:0
    })
    .on('select2:select', function(e){
      const product_name = $('#id_product_name').val(),
            stage_name   = $('#id_stage_name').val(),
            batch_no     = e.params.data.id;
  
      // fetch voucher
      fetch('/get_voucher_details_by_batch/', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({product_name,stage_name,batch_no})
      })
      .then(r=>r.json())
      .then(d=>{ if(d.voucher_no) $('#id_voucher_no').val(d.voucher_no); });
  
      // fetch rows
      fetch('/get_effluent_qty_details/', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({product_name,stage_name,batch_no})
      })
      .then(r=>r.json())
      .then(renderRows);
    });
  
  
    // ── 2) Row‐rendering function ────────────────────────────────────────────────
    // clears #form-container and appends 5‐col rows including quantity_kg
    function renderRows(json){
      if(!json.data) return;
      const prefix    = 'effluentqty_set',
            $total    = $(`#id_${prefix}-TOTAL_FORMS`),
            $container= $('#form-container');
    
      $container.empty();
    
      json.data.forEach((item,i)=>{
        const actualQty = parseFloat(item.actual_quantity || 0);
        const density   = parseFloat(item.density || 0);
        const quantityKg = (actualQty * density * 1000).toFixed(0);
    
        const row = $(`
          <div class="form-row relative grid grid-cols-5 gap-4 border p-4 rounded-lg bg-gray-50">
            <div>
              <label>Category</label>
              <input type="text" name="${prefix}-${i}-category" value="${item.category}"
                     readonly class="w-full p-2 border rounded bg-gray-50"/>
            </div>
            <div>
              <label>Nature</label>
              <input type="text" name="${prefix}-${i}-effluent_nature" value="${item.effluent_nature}"
                     readonly class="w-full p-2 border rounded bg-gray-50"/>
            </div>
            <div>
              <label>Planned Qty</label>
              <input type="number" step="0.01" name="${prefix}-${i}-plan_quantity"
                     value="${item.plan_quantity}" readonly class="w-full p-2 border rounded bg-gray-50"/>
            </div>
            <div>
              <label>Actual Qty</label>
              <input type="number" step="0.01" name="${prefix}-${i}-actual_quantity"
                     value="${actualQty}" class="w-full p-2 border rounded"/>
            </div>
            <div>
              <label>Qty (kg)</label>
              <input type="number" step="0.01" name="${prefix}-${i}-quantity_kg"
                     value="${quantityKg}" readonly class="w-full p-2 border rounded bg-gray-50"/>
            </div>
    
            <input type="hidden" name="${prefix}-${i}-density" value="${density}" />
            <input type="hidden" name="${prefix}-${i}-id" />
    
            <!-- ✅ Remove Button -->
            <button type="button"
                    class="absolute -top-2 -right-2 bg-red-600 text-white rounded-full w-6 h-6 flex items-center justify-center remove-row"
                    title="Remove Row">&minus;</button>
          </div>
        `);
    
        $container.append(row);
      });
    
      $total.val(json.data.length);
    }
    
  
  
    // ── 3) Add‐row button ────────────────────────────────────────────────────────
    const prefix      = 'effluentqty_set',
          $totalForms = $(`#id_${prefix}-TOTAL_FORMS`),
          $container  = $('#form-container'),
          $btnAdd     = $('#add-row');
  
  
  
    // ── 4) Auto‐calc Qty (kg) on Actual change ───────────────────────────────────
    $container.on('input', `input[name^="${prefix}"][name$="-actual_quantity"]`, function(){
      const idx     = this.name.match(/-(\d+)-/)[1],
            actual  = parseFloat(this.value)||0,
            density = parseFloat($(`input[name="${prefix}-${idx}-density"]`).val())||0,
            result  = (actual * density * 1000).toFixed(0);  // Remove decimal from final qty
      $(`input[name="${prefix}-${idx}-quantity_kg"]`).val(result);
    });
    
    
     // ── 5) Remove row from formset and re-index ─────────────────────────────────
    $(document).on('click', '.remove-row', function(){
      const $rows = $('#form-container .form-row');

      if ($rows.length <= 1) {
        alert('⚠️ At least one row is required.');
        return;
      }

      // Remove row
      const $row = $(this).closest('.form-row');
      $row.remove();

      // Re-index remaining rows
      const prefix = 'effluentqty_set';
      const $forms = $('#form-container .form-row');
      $forms.each(function(i){
        $(this).find(':input').each(function(){
          const name = $(this).attr('name');
          if(name) $(this).attr('name', name.replace(/-\d+-/, `-${i}-`));
        });
      });

      $(`#id_${prefix}-TOTAL_FORMS`).val($forms.length);
    });
        

    $btnAdd.click(function(){
      let count = +$totalForms.val();
      let $new = $('#empty-form > .form-row').clone();
    
      // Replace __prefix__ in all input/select/textarea name and id
      $new.find(':input, select, textarea').each(function(){
        const $el = $(this);
        const name = $el.attr('name');
        const id   = $el.attr('id');
    
        if (name) $el.attr('name', name.replace(/__prefix__/, count));
        if (id)   $el.attr('id', id.replace(/__prefix__/, count));
    
        // Clear value unless it's a hidden field like density or id
        if (!$el.is('[type="hidden"]')) $el.val('');
      });
    
      // Force reset of select options (especially needed for Django validation)
      $new.find('select').each(function(){
        $(this).find('option:first').prop('selected', true);  // default to empty
      });
    
      // Append to formset container and update TOTAL_FORMS count
      $container.append($new);
      $totalForms.val(count + 1);
    });


    const NATURE_MAP = {
      "Process": [
        "Acidic", "Basic", "Neutral", "Sodium Cyanide Effluent", "3CHP effluent",
        "Ammonium Chloride effluent", "Spent Sulphuric Acid", "Residue"
      ],
      "Unprocess": [
        "Scrubber HCl 30-32%", "Scrubber Basic Effluent", "Scrubber Acidic Effluent",
        "QC effluent", "Outside Drainage Water", "Dyke Effluent",
        "PCO Cleaning / cleaning Effluent", "Ejector effluent", "Scrubber Nox effluent"
      ]
    };

    // Dynamically update nature options based on selected category
    $(document).on('change', '.category-select', function() {
      const selectedCat = $(this).val();
      const $nature = $(this).closest('.form-row').find('.nature-select');

      $nature.empty().append('<option value="">–– Select Nature ––</option>');
      if (NATURE_MAP[selectedCat]) {
        NATURE_MAP[selectedCat].forEach(n => {
          $nature.append(`<option value="${n}">${n}</option>`);
        });
      }
    });


// Edit page
    function prefillSelect2(id, text) {
      if (text) {
          const newOption = new Option(text, text, true, true);
          $(`#${id}`).append(newOption).trigger('change');
      }
  }
  
  if (window.location.href.includes('/edit/')) {
      prefillSelect2('id_product_name', $('#init_product_name').val());
      prefillSelect2('id_stage_name', $('#init_stage_name').val());
      prefillSelect2('id_batch_no', $('#init_batch_no').val());
  }

});
  