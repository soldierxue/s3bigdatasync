var data = {};

function getTotalProgressValueFromDDB(callback) {
    var obj = new XMLHttpRequest();
    obj.open("GET", APIEndpoint + "/totalProgress", true);
    obj.setRequestHeader("Content-type", "application/json");
    
    obj.onreadystatechange = function() {
        if (obj.readyState == 4 && (obj.status == 200 || obj.status == 304 || obj.status == 201)) {
            if obj.responseText != {} {
                data = obj.responseText;
                callback();
            }
        }
    };
    obj.send(null);
}

function updateTotalProgress() {
    if data != {} {
        var successSizeData = getNumberAndUnitFromBytes(data.successSize);
        document.getElementById("success-progress-size").innerHTML = successSizeData[0];
        document.getElementById("success-progress-unit").innerHTML = successSizeData[1];
        
        var totalSizeData = getNumberAndUnitFromBytes(data.totalSize);
        document.getElementById("total-progress-size").innerHTML = totalSizeData[0];
        document.getElementById("total-progress-unit").innerHTML = totalSizeData[1];
        
        var progressPercent = Math.round(data.successSize / data.totalSize * 100);
        document.getElementById("total-progress-bar").setAttribute("aria-valuenow", progressPercent);
        document.getElementById("total-progress-bar").style.width = progressPercent + "%";
        document.getElementById("total-progress-bar").innerHTML = progressPercent + "%";
        
        var estimateSpeedData = getNumberAndUnitFromBytes(data.estimateSpeed);
        document.getElementById("current-speed-value").innerHTML = estimateSpeedData[0];
        document.getElementById("current-speed-unit").innerHTML = estimateSpeedData[1] + "/min";
        
        var startTime = new Date();
        startTime.setTime(data.startTime * 1000);
        document.getElementById("start-time").innerHTML = startTime.Format("yyyy/MM/dd hh:mm");
        
        if (data.successSize == data.totalSize) {
            document.getElementById("project-status").innerHTML = "Success";

            document.getElementById("expected-end-time").innerHTML = "NaN";
        } else {
            document.getElementById("project-status").innerHTML = "In progress";
            
            estimateTimeSpend = Date.parse(new Date()) + (data.totalSize - data.successSize) / estimateSpeed * 60000;
            var estimateTime = new Date();
            estimateTime.setTime(estimateTimeSpend);
            
            document.getElementById("expected-end-time").innerHTML = estimateTime.Format("yyyy/MM/dd hh:mm");
        }
        
        document.getElementById("transfered-objects").innerHTML = data.successObjects;
        document.getElementById("total-objects").innerHTML = data.totalObjects;
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

getTotalProgressValueFromDDB(function() {
                             window.onload = function() {
                                updateTotalProgress();
                             
                                var totalProgressAutoUpdate = window.setInterval("getTotalProgressValueFromDDB(updateTotalProgress)", 60000);
                             }
                            })
