const videoElement = document.getElementById("webcam");
videoElement.style.transform = "scaleX(-1)";
camera = 0;
shouldScan = true;
let lastCode = "";


let location_rolls = {}

function selectLocation(el) {
    let oldSelected = document.getElementsByClassName("locationBtn active")
    document.getElementById("itemsTable").classList.remove("noActive")
    document.getElementById("itemsTable").classList.remove("active")
    document.getElementById("itemsTitle").innerHTML = "Rolls in " + el.innerHTML
    document.getElementById("webcam").classList.remove("hide")

    setTimeout(() => {
        let tableData = `<tr>
                    <th>Roll</th>
                    <th>Location</th>
                    <th>Status</th>
                </tr>`
                console.log(location_rolls[el.innerHTML])
        location_rolls[el.innerHTML].forEach(roll => {
            tableData = tableData + `
        <tr>
            <td id='${roll['roll'].split(':')[0]}'>${roll["roll"]}</td>
            <td >${roll["location"]}</td>
                <td>
                    <div id='${roll['roll'].split(':')[0] + "st"}' class="statusRoll ${roll['status'] === 'To Do'? "toDo" : "done"}">
                        ${roll["status"]}
                    </div>
            </td>
        </tr>
        `
        });

        document.getElementById("itemsTable").innerHTML = tableData
        document.getElementById("itemsTable").classList.add("active")
    }, 400);
    if (oldSelected.length > 0) {
        oldSelected[0].classList.remove("active")
    }

    if (!el.classList.contains("active")) {
        el.classList.add("active")
    }

}

function selectOption(el) {
    setTimeout(() => {
        closeMenu()
    }, 300);
    
    console.log(el.innerHTML)
    document.getElementById("queueTitleP").innerText = el.innerHTML + " Queue"
    document.getElementById("itemsTable").classList.remove("active")
    document.getElementById("itemsTitle").innerHTML = ""
    document.getElementById("webcam").classList.add("hide")
    $.ajax({
        type: "POST",
        url: "/warehouse/checkout/search_queue",
        data: JSON.stringify(el.innerHTML),
        contentType: "application/json",
        dataType: 'json',
        success: function (result) {
            location_rolls = result
            console.log(location_rolls)
            let locations = ""
            for (const property in result) {
                if (property === 'status') {
                    locations = locations + `<button class="locationBtn">Empty Queue</button>`
                    break
                }
                locations = locations + `<button onclick="selectLocation(this)" class="locationBtn">${property}</button>`
            }
            document.getElementById("locationContainer").innerHTML = locations
        }
    });
}
document.getElementById("menuBtn").addEventListener("focusout", (event) => {
    closeMenu()
});
function closeMenu(){
    let menuBtn = document.getElementById("menuBtn")
    menuBtn.classList.remove("active")
        document.getElementById("l1").classList.remove("active");
        document.getElementById("l2").classList.remove("active");
        document.getElementById("l3").classList.remove("active");
        document.getElementById("menuContainer").classList.remove("active");
        document.getElementById("menuContainer").classList.add("noActive");
}

function openMenu() {
    let menuBtn = document.getElementById("menuBtn")
    menuBtn.classList.add("active")
        document.getElementById("l1").classList.add("active");
        document.getElementById("l2").classList.add("active");
        document.getElementById("l3").classList.add("active");
        document.getElementById("menuContainer").classList.add("active");
        document.getElementById("menuContainer").classList.remove("noActive");
}
function menuBtnFunction() {
   
    if (!menuBtn.classList.contains("active")) {
       openMenu()
    } else if (menuBtn.classList.contains("active")) {
        closeMenu()
    }
}


async function getWebcamStream(camera) {
  try {
    const devices = await navigator.mediaDevices.enumerateDevices();
    const videoDevices = devices.filter(
      (device) => device.kind === "videoinput"
    );
    if (videoDevices.length >= 2) {
      camera = 1;
      videoElement.style.transform = "scaleX(1)";
    }
    const constraint = {
      video: {
        deviceId: videoDevices[camera].deviceId,
        advanced: [{ zoom: { min: 1, max: 5 } }],
      },
    };

    const stream = await navigator.mediaDevices.getUserMedia(constraint);
    videoElement.srcObject = stream;
    console.log(videoElement.style.transform);
    const track = videoElement.srcObject.getVideoTracks()[0];
  const capabilities = track.getCapabilities();
  if (capabilities.zoom) {
    track.applyConstraints({ advanced: [{ zoom: 4 }] })
      .then(() => console.log(`Zoom level set to ${zoomValue}`))
      .catch((error) => console.error("Error setting zoom:", error));
  }

    videoElement.onloadedmetadata = () => {
      const canvas = document.createElement("canvas");
      const context = canvas.getContext("2d");

      function processVideoFrame() {
        try {
          canvas.width = videoElement.width;
          canvas.height = videoElement.height;
          context.drawImage(videoElement, 0, 0, canvas.width, canvas.height);

          const imageData = context.getImageData(
            0,
            0,
            canvas.width,
            canvas.height
          );
          const code = jsQR(imageData.data, imageData.width, imageData.height);
          if (code) {
            
            if (validateCode(code.data) && lastCode !== code.data) {
              lastCode = code.data;
              // playSound()
              // Stop scanning when a QR code is detected
              shouldScan = false;

              // Create a data object to send via POST
              var data = {
                code: code.data,
              };

              if (document.getElementById("queueTitleP").innerText.includes('Inspection')){
                payload = {
                    roll: code.data,
                    locationName:"inspection",
                    queue: document.getElementById("queueTitleP").innerText,
                  };
              } else {
                payload = {
                    roll: code.data,
                    locationName:
                      document.getElementsByClassName("locationBtn active")[0]
                        .innerHTML,
                    queue: document.getElementById("queueTitleP").innerText,
                  };
              }
              console.log(payload)
              // Perform an AJAX POST request
              $.ajax({
                type: "POST",
                url: "/warehouse/checkout_logic",
                data: JSON.stringify(payload),
                contentType: "application/json",
                success: function (response) {
                  if (response.status === "Done") {
                    statusElemet = document.getElementById(
                        response.roll.split(":")[0] + "st"
                      )
                     statusElemet.innerHTML = "Done";
                     statusElemet.classList.remove('toDo')
                     statusElemet.classList.add('done')
                    location_rolls[document.getElementsByClassName("locationBtn active")[0].innerHTML].forEach(element => {
                        if(element.roll === response.roll){
                            element.status = 'Done'
                        }
                    });
                    swal.fire({
                      title: "Correcto",
                      text: response.message,
                      icon: "success",
                      backdrop: false,
                      confirmButtonText: "Ok",
                    });
                  }

                  if (response.status == "Error") {
                    swal
                      .fire({
                        title: "Error",
                        text: response.message,
                        icon: "error",
                        backdrop: false,
                      })
                      .then((result) => {
                        lastCode = "";
                      });
                  }
                },
                error: function (error) {
                  console.log(error);
                  lastCode = ""; // Resume scanning even in case of an error
                  swal.fire({
                    title: "Error",
                    text: "Algo salió mal",
                    icon: "error",
                    backdrop: false,
                  });
                },
              });
            } else {
              
            }
          }
        } catch (error) {}

        requestAnimationFrame(processVideoFrame);
      }

      function validateCode(scannedCode) {
        const pattern1 = /^[A-Za-z]{1,2}-[A-Za-z]-\d$/;
        const pattern2 = /^R\d{5}$/;

        if (pattern1.test(scannedCode) || pattern2.test(scannedCode)) {
          return true;
        } else {
          return false;
        }
      }

      requestAnimationFrame(processVideoFrame);
    };
  } catch (error) {
    console.error("Error accessing webcam:", error);
  }
}
getWebcamStream(camera);

function playSound() {
  var audio = document.getElementById("myAudio");
  audio.play();
}
