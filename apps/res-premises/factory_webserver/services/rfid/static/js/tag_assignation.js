const videoElement = document.getElementById("webcam");

videoElement.style.transform = "scaleX(-1)";

async function getWebcamStream() {
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ video: true });
    videoElement.srcObject = stream;

    videoElement.onloadedmetadata = () => {
      const canvas = document.createElement("canvas");
      const context = canvas.getContext("2d");

      function processVideoFrame() {
        canvas.width = videoElement.videoWidth;
        canvas.height = videoElement.videoHeight;
        context.drawImage(videoElement, 0, 0, canvas.width, canvas.height);

        const imageData = context.getImageData(
          0,
          0,
          canvas.width,
          canvas.height
        );
        const code = jsQR(imageData.data, imageData.width, imageData.height);

        if (code) {
          if (validateValue(code.data)) {
            playSound();
            document.getElementById("roll").value = code.data;
          } else {
            console.log("Input value is not valid.");
          }
        }

        requestAnimationFrame(processVideoFrame);
      }

      function validateValue(inputValue) {
        if (inputValue.length !== 6) {
          return false;
        }

        if (inputValue.charAt(0) !== "R") {
          return false;
        }

        const digits = inputValue.substring(1);
        for (let i = 0; i < 5; i++) {
          if (digits.charAt(i) < "0" || digits.charAt(i) > "9") {
            return false;
          }
        }

        return true;
      }

      requestAnimationFrame(processVideoFrame);
    };
  } catch (error) {
    console.error("Error accessing webcam:", error);
  }
}

function clearTag1() {
  document.getElementById("tag1").value = "";
}

function clearTag2() {
  document.getElementById("tag2").value = "";
}

function fetchEpc() {
  fetch("/rfid/get_epc")
    .then((response) => response.json())
    .then((data) => {
      if (data.item) {
        swal.fire({
          title: "Tag ya asignado",
          html: `El tag: <b><span style="color: red">${data.tag}</span></b> está asignado al rollo: <b><span style="color: red">${data.item}</span></b>. <br/><br/>Si esto le impide continuar, comuníquese con el equipo de TI.`,
          icon: "error",
          backdrop: false,
          confirmButtonText: "OK",
        });
      } else {
        if (
          document.getElementById("tag1").value == "" &&
          document.getElementById("tag2").value != data.tag
        ) {
          document.getElementById("tag1").value = data.tag;
        } else if (
          document.getElementById("tag2").value == "" &&
          document.getElementById("tag1").value != data.tag
        ) {
          document.getElementById("tag2").value = data.tag;
        }
      }
    })
    .catch((error) => console.error("Error fetching data:", error));
}

function sendEvent() {
  const postData = {
    roll: document.getElementById("roll").value,
    tag1: document.getElementById("tag1").value,
    tag2: document.getElementById("tag2").value,
  };

  if (postData.roll !== "" && postData.tag1 !== "" && postData.tag2 !== "") {
    const loadingPopup = swal.fire({
      html:
        '<div class="custom-loading-wrapper">' +
        '<img src="static/loading.gif" class="custom-loading-gif" alt="Loading..."><br/>' +
        "<h2>Loading</h2><br/>" +
        "<div>Assigning tags to roll</div>" +
        "</div>",
      showCancelButton: false,
      showConfirmButton: false,
      allowOutsideClick: false,
      backdrop: false,
      onBeforeOpen: () => {
        swal.showLoading();
      },
    });

    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/rfid/roll_tag_assignation", true);
    xhr.setRequestHeader("Content-Type", "application/json");

    xhr.onreadystatechange = function () {
      if (xhr.readyState === XMLHttpRequest.DONE) {
        loadingPopup.close();

        if (xhr.status === 200) {
          document.getElementById("roll").value = "";
          document.getElementById("tag1").value = "";
          document.getElementById("tag2").value = "";
          swal.fire({
            title: "Success",
            text: "Tags were assigned correctly!",
            icon: "success",
            backdrop: false,
          });
        } else {
          swal.fire({
            title: "Error",
            text: "Tags were assigned correctly!",
            icon: "error",
            backdrop: false,
          });
        }
      }
    };

    xhr.send(JSON.stringify(postData));
  } else {
    swal.fire({
      title: "Error",
      text: "All fields must be filled.",
      icon: "error",
      backdrop: false,
    });
  }
}

function playSound() {
  var audio = document.getElementById("myAudio");
  audio.play();
}

document.getElementById("tag1").addEventListener("click", clearTag1);
document.getElementById("tag2").addEventListener("click", clearTag2);
document.getElementById("ok").addEventListener("click", sendEvent);

getWebcamStream();
setInterval(fetchEpc, 1000);
