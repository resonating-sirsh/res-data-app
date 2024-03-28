let button = document.getElementById("btn-search-roll");
let checks = [];
button.addEventListener("click", () => {
  document.getElementById("weight").value = "";
  valor = document.getElementById("inputSelected").value;

  if (valor === "" || document.getElementById("select-process").value === "") {
    alert("Por favor seleccionar Procedimiento y rollo");
    return;
  }

  document.getElementById("roll-name").innerHTML = valor;

  fetch("search_jobs", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ roll: valor }),
  })
    .then((response) => response.json())
    .then((data) => {
      jobs_list = [];
      data.forEach((element) => {
        jobs_list.push({
          id: element.id,
          name: element.fields["Printed Name"],
          checked:
            element.fields[
              `Home Wash ${
                document.getElementById("process-name").innerHTML === "Empacar"
                  ? "Packed"
                  : "Received"
              }`
            ],
        });
      });

      document.getElementById("pending-jobs-table").innerHTML = "";
      document.getElementById("check-jobs-table").innerHTML = "";
      let jobs_table = ``;
      jobs_list.forEach((element) => {
        jobs_table =
          jobs_table +
          `
        <tr id="${"tr-" + element.name}">
          <td><input type="checkbox" name="job-check" id="${
            element.name
          }" ></td>
          <td id="${"t-" + element.name}">${element.name}</td>
        </tr>`;
      });
      document.getElementById("pending-jobs-table").innerHTML += jobs_table;
      document.getElementById("inputSelected").value = null;

      checks = document.getElementsByName("job-check");

      checks.forEach((element) => {
        element.addEventListener("change", (event) => {
          toggleRow(event);
          fetch("check_roll", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              name: event.target.id,
              type:
                document.getElementById("select-process").value === "empacar"
                  ? "Packed"
                  : "Received",
              value: event.target.checked,
            }),
          });

          if (
            document.getElementById("pending-jobs-table").children.length === 0
          ) {
            //List complete
          }
        });
      });

      document.getElementById("job-text").focus();
    })
    .catch((error) => {
      console.error("Error:", error);
    });
});

document
  .getElementById("job-text")
  .addEventListener("keypress", function (event) {
    // If the user presses the "Enter" key on the keyboard
    if (event.key === "Enter") {
      job = document.getElementById(event.target.value);
      event.target.value = "";
      if (!job) {
        alert("Job no existe");
      }
      if (!job.checked) {
        job.click();
      }
    }
  });

let toggleRow = (element) => {
  document
    .getElementById(`t-${element.target.id}`)
    .classList.toggle("job-checked");

  tr = document.getElementById(`tr-${element.target.id}`);

  table = tr.parentNode;

  table.removeChild(tr);
  if (element.target.checked) {
    document.getElementById("check-jobs-table").prepend(tr);
  } else {
    document.getElementById("pending-jobs-table").prepend(tr);
  }
};

function generatePDF() {
  if (document.getElementById("process-name").innerHTML === "empacar") {
    // Choose the element that your content will be rendered to.
    const element = document.getElementById("check-jobs-table");
    // Choose the element and save the PDF for your user.

    jobs_list = ``;
    jobs = document.getElementById("check-jobs-table").children;

    jobs.forEach((job) => {
      console.log(job.children[1].innerHTML);
      jobs_list =
        jobs_list +
        `
  <tr>
  <td>${job.children[1].innerHTML}</td>
  <td><input type="checkbox"></td>
</tr>
  `;
    });

    html = `
<style>
td, th {
  border: 1px solid #ddd;
  padding: 8px;
  height: auto;
  word-wrap: break-word;
}
</style>

<table>
<tr>
  <th colspan="2">${document.getElementById("roll-name").innerHTML} || Peso: ${
      document.getElementById("weight").value
    }kg</th>
</tr>
<tbody>${jobs_list}</tbody>
</table>
`;
    var opt = {
      margin: 0.7,
      image: { type: "jpeg", quality: 1 },
      filename: "myfile.pdf",
      jsPDF: { unit: "in", format: "letter", orientation: "portrait" },
    };
    html2pdf()
      .set(opt)
      .from(html)
      .toPdf()
      .get("pdf")
      .then(function (pdfObj) {
        pdfObj.autoPrint();
        window.open(pdfObj.output("bloburl"), "_blank");
      });
  }
}

document
  .getElementById("select-process")
  .addEventListener("change", (element) => {
    document.getElementById("process-name").innerHTML = element.target.value;
  });

document.getElementById("btn-print").addEventListener("click", () => {
  weight = document.getElementById("weight").value;
  if (weight) {
    fetch("weight", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: document.getElementById("roll-name").innerHTML,
        weight: weight,
      }),
    });
    generatePDF();
  } else {
    alert("Ingrese un peso");
  }
});
