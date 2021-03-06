var progressData = {};

function getTotalProgressValueFromDDB(callback) {
    if (window.XMLHttpRequest) {
        var obj = new XMLHttpRequest();
    } else if (window.ActiveXObject) {
        var obj = new ActiveXObject("Microsoft.XMLHTTP");
    }
    obj.open("GET", APIEndpoint + "totalProgress", true);
    
    obj.onreadystatechange = function() {
        if (obj.readyState == 4 && (obj.status == 200 || obj.status == 304 || obj.status == 201)) {
            if (obj.responseText != "{}\n") {
                progressData = eval("(" + obj.responseText + ")");
                console.log(progressData);
            } else {
                console.log({})
            }
            callback();
        }
    };
    obj.send(null);
}

function updateTotalProgress() {
    if (JSON.stringify(progressData) != "{}") {
        var successSizeData = getNumberAndUnitFromBytes(progressData.successSize);
        document.getElementById("success-progress-size").innerHTML = successSizeData[0];
        document.getElementById("success-progress-unit").innerHTML = successSizeData[1];
        
        var totalSizeData = getNumberAndUnitFromBytes(progressData.totalSize);
        document.getElementById("total-progress-size").innerHTML = totalSizeData[0];
        document.getElementById("total-progress-unit").innerHTML = totalSizeData[1];
        
        var progressPercent = Math.round(progressData.successSize / progressData.totalSize * 100);
        document.getElementById("total-progress-bar").setAttribute("aria-valuenow", progressPercent);
        document.getElementById("total-progress-bar").style.width = progressPercent + "%";
        document.getElementById("total-progress-bar").innerHTML = progressPercent + "%";
        
        var estimateSpeedData = getNumberAndUnitFromBytes(progressData.estimateSpeed);
        document.getElementById("current-speed-value").innerHTML = estimateSpeedData[0];
        document.getElementById("current-speed-unit").innerHTML = estimateSpeedData[1] + " / min";
        
        var startTime = new Date();
        startTime.setTime(progressData.startTime * 1000);
        document.getElementById("start-time").innerHTML = startTime.Format("yyyy/MM/dd hh:mm");
        
        if (progressData.successSize == progressData.totalSize) {
            document.getElementById("project-status").innerHTML = "Success";

            document.getElementById("expected-end-time").innerHTML = "NaN";
        } else {
            document.getElementById("project-status").innerHTML = "In progress";
            
            estimateTimeNeeded = (progressData.totalSize - progressData.successSize) / progressData.estimateSpeed * 60;
            if (estimateTimeNeeded > 86400) {
                document.getElementById("expected-end-time").innerHTML = "NaN";
            } else {
                estimateTimeSpend = Date.parse(new Date()) + estimateTimeNeeded * 1000;
                var estimateTime = new Date();
                estimateTime.setTime(estimateTimeSpend);
                console.log(estimateTimeSpend);
                
                document.getElementById("expected-end-time").innerHTML = estimateTime.Format("yyyy/MM/dd hh:mm");
            }
        }
        
        document.getElementById("transfered-objects").innerHTML = progressData.successObjects;
        document.getElementById("total-objects").innerHTML = progressData.totalObjects;
    } else {
        document.getElementById("project-status").innerHTML = "Pending";
    }
}

function getNumberAndUnitFromBytes(value) {
    const BytesUnitArray = ["B", "KB", "MB", "GB", "TB", "PB"]
    
    for (var i = 5; i > 0; i--) {
        var checkedValue = Math.round(value / Math.pow(BytesConverterNumber, i) * 10) / 10;
        
        if (checkedValue >= 1) {
            var checkedValueString = checkedValue.toString();
            var index = checkedValueString.indexOf('.');
            if (index < 0) {
                checkedValueString += '.0';
            }
            
            return [checkedValueString, BytesUnitArray[i]];
        }
    }
    
    return [value, "B"];
}

Date.prototype.Format = function (fmt) {
    var o = {
        "M+": this.getMonth() + 1,
        "d+": this.getDate(),
        "h+": this.getHours(),
        "m+": this.getMinutes(),
        "s+": this.getSeconds(),
        "q+": Math.floor((this.getMonth() + 3) / 3),
        "S": this.getMilliseconds()
    };
    if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    for (var k in o)
        if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
    return fmt;
}

window.onload = function() {
    getTotalProgressValueFromDDB(updateTotalProgress);
    
    var totalProgressAutoUpdate = window.setInterval("getTotalProgressValueFromDDB(updateTotalProgress)", 60000);
}
