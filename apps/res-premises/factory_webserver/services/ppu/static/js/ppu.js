const videoElement = document.getElementById('webcam');
videoElement.style.transform = 'scaleX(-1)';
camera = 0
// Flag to control QR code scanning
let shouldScan = true;
// Obtener la lista de dispositivos de entrada de video
function test() {
    navigator.mediaDevices.enumerateDevices()
        .then(devices => {
            // Filtrar solo los dispositivos de video
            const videoDevices = devices.filter(device => device.kind === 'videoinput');

            // Separar las cámaras frontales de las traseras
            const frontCameras = videoDevices.filter(device => device.label.toLowerCase().includes('front'));
            const rearCameras = videoDevices.filter(device => !device.label.toLowerCase().includes('front'));

            // Imprimir la información en la consola
            console.log('Cámaras frontales:', frontCameras);
            console.log('Cámaras traseras:', rearCameras);

            // Puedes hacer más acciones con esta información aquí
        })
        .catch(error => {
            console.error('Error al enumerar dispositivos:', error);
        });
}

async function getWebcamStream(camera) {
    try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        const videoDevices = devices.filter(device => device.kind === 'videoinput');
        if (videoDevices.length >= 2) {
            //console.log(videoDevices)
            /*                     document.getElementById('switchCamera').style.display = 'block';
                                if (camera == 0) {
                                    videoElement.style.transform = 'scaleX(1)';
                                } else if (camera == 1) {
                                    videoElement.style.transform = 'scaleX(-1)';
                                } */
            camera = 1
            videoElement.style.transform = 'scaleX(1)';

        }
        const constraint = {
            video: {
                deviceId: videoDevices[camera].deviceId,
            }
        };

        const stream = await navigator.mediaDevices.getUserMedia(constraint);
        videoElement.srcObject = stream;
        console.log(videoElement.style.transform)

        videoElement.onloadedmetadata = () => {
            const canvas = document.createElement('canvas');
            const context = canvas.getContext('2d');

            function processVideoFrame() {

                canvas.width = videoElement.videoWidth;
                canvas.height = videoElement.videoHeight;
                context.drawImage(videoElement, 0, 0, canvas.width, canvas.height);

                const imageData = context.getImageData(0, 0, canvas.width, canvas.height);
                const code = jsQR(imageData.data, imageData.width, imageData.height);

                if (code) {

                    if (shouldScan == true && validateValue(code.data)) {
                        // Stop scanning when a QR code is detected
                        shouldScan = false;

                        // Create a data object to send via POST
                        var data = {
                            value: code.data
                        };

                        // Perform an AJAX POST request
                        $.ajax({
                            type: "POST",
                            url: "/ppu_creation/ppu_qr_code",
                            data: JSON.stringify(data),
                            contentType: "application/json",
                            success: function (response) {
                                if (response.status == 'Success') {
                                    $("#next_roll").html(response.roll_name);
                                    swal.fire({
                                        title: "Success",
                                        text: response.message,
                                        icon: "success",
                                        backdrop: false,
                                        confirmButtonText: 'Ok',
                                    }).then((result) => {
                                        shouldScan = true;
                                    })
                                } else if (response.status == 'Error') {
                                    console.log("error" + shouldScan)
                                    swal.fire({
                                        title: "Error",
                                        text: response.message,
                                        icon: "error",
                                        backdrop: false,
                                        confirmButtonText: 'Ok',
                                    }).then((result) => {
                                        shouldScan = true
                                    })
                                }
                            },
                            error: function (error) {
                                console.log(error);
                                shouldScan = true; // Resume scanning even in case of an error
                                swal.fire({
                                    title: "Error",
                                    text: response.message,
                                    icon: "error",
                                    backdrop: false
                                });
                            }
                        });
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

                if (inputValue.charAt(0) !== 'R') {
                    return false;
                }

                const digits = inputValue.substring(1);
                for (let i = 0; i < 5; i++) {
                    if (digits.charAt(i) < '0' || digits.charAt(i) > '9') {
                        return false;
                    }
                }

                return true;
            }

            requestAnimationFrame(processVideoFrame);
        };
    } catch (error) {
        console.error('Error accessing webcam:', error);
    }
}



$(document).ready(function () {
    // Attach an event handler to all buttons with the "type_btn" class
    $(".type_btn").click(function () {
        // Get the value of the data-value attribute of the clicked button
        var value = $(this).data("value");

        // Create a data object to send via POST
        var data = {
            value: value
        };

        // Perform an AJAX POST request
        $.ajax({
            type: "POST",
            url: "/ppu_creation", // Replace with your server URL
            data: JSON.stringify(data),
            contentType: "application/json",
            success: function (response) {
                $("#next_roll").html(response.roll_name);
                $(".type_btn").hide();
            },
            error: function () {
                // In case of an error
                $("#result").text("Error in AJAX request.");
            }
        });
    });
    $("#switchCamera").click(function () {
        // Get the value of the data-value attribute of the clicked button
        var value = $(this).val();
        getWebcamStream(value);
        if (value == 0) {
            $(this).val(1);
        } else {
            $(this).val(0);
        }

    });

});


getWebcamStream(camera);
setInterval(test, 1000);