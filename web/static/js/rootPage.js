let ip_addr;
let web_port;
let workers;
let workersDetail = {};
let struDetail = {};
let serverTimes = [];
let serverCurrentTime;
let errors = [];
let deadList = [];
let deadServerTimes = [];
let FindBlockAndSuccess ;
let tfbContentTermBTNFocus = "Total";
let tsContentTermBTNFocus = "Total";
let raceWinMachinesTermBTNFocus = "Total";
let logFocus = 0;
let logLastTime;
let logData;

$(document).ready(function (){
     ip_addr = $("#ip_addr").text();
     web_port = $("#web_port").text();
//    ip_addr = "127.0.0.1"
//    web_port = 2648
    setStaticArea();
    init();
    hideLoadingPanel();
    checkErrors();
    reloadDatas();
    checkDead();
    setInterval(reloadDatas, 10000);
    setInterval(reloadBigDatas, 1000000);
    setInterval(poolingLogs, 2000);
})

function init(){
    getWorkers(true);
    getWorkersDetail(true);
    getStruDetail(true);
    getCurrentBlockInfo(true);
    getFindBlockAndSuccess(true);
    reFreshDropDownData();
    getLogs(true);
}

function reloadDatas(){
    getWorkers(false);
    getWorkersDetail(false);
    getStruDetail(false);
    checkDead();
    getCurrentBlockInfo(false);
}

function reloadBigDatas(){
    getFindBlockAndSuccess(false);
}

function poolingLogs(){
    if (logFocus !=0){
        getLogs(false);
}
    }



window.addEventListener('resize', setStaticArea);

function setStaticArea(){
    var height = $('.staticArea').height();
    $(".webContent").css('margin-top',height);
}

function hideLoadingPanel(){
    $(".loadingPanel").addClass("hidden");
}

function checkErrors(){
    if (errors.length>0){
        $(".errorPanel").show();
        $("#errMsg").empty();
        for (var i =0; i < errors.length ; i++){
            $("#errMsg").append("<p> <ion-icon name='close-outline' style='font-size: 15px;'></ion-icon> "+errors[i]+"</p>");
        }

    }
}

function getWorkers(isInit){
    $.ajax({
        url: 'http://'+ip_addr+':'+web_port+'/api/getWorkerNodes', // 서버 API 엔드포인트 URL
        type: 'GET', // HTTP 요청 방법 (GET, POST 등)
        dataType: 'json', // 응답 데이터 유형 (JSON)
        async: !isInit,
        success: function(data) { // 요청 성공 시 콜백 함수
            // 서버로부터 받은 JSON 데이터를 처리합니다.
            workers = data.result.sort();
        },
        error: function(xhr, status, error) { // 요청 실패 시 콜백 함수
            // 오류 처리
            errors.push("("+status+")"+error);
        }
    });
    // wait(5);
}

function getWorkersDetail(isInit){
    $.ajax({
        url: 'http://'+ip_addr+':'+web_port+'/api/getWorkersDetails', // 서버 API 엔드포인트 URL
        type: 'POST', // HTTP 요청 방법 (GET, POST 등)
        contentType:'application/json',
        dataType: 'json', // 응답 데이터 유형 (JSON)
        data: JSON.stringify(workers),
        async: !isInit,
        success: function(data) { // 요청 성공 시 콜백 함수
            // 서버로부터 받은 JSON 데이터를 처리합니다.
            workersDetail = data.result;
        },
        error: function(xhr, status, error) { // 요청 실패 시 콜백 함수
            // 오류 처리
            errors.push("("+status+")"+error);
        }
    });
}

function drawRaceWinMachinesChart(inValue){
    // $("#raceWinMachinesChart").remove;
    // $("#raceWinMachinesChartArea").append("<canvas id='raceWinMachinesChart' width='100%'></canvas>");

    var raceWinMachinesChart = $("#raceWinMachinesChart");
    var tmpList = [];
    var winWorkers = [];
    var data = [];

    var chartStatus = Chart.getChart("raceWinMachinesChart");
    if (chartStatus != undefined){
        chartStatus.destroy();
    }

    for(var find in FindBlockAndSuccess["find"][inValue]){
        if (FindBlockAndSuccess.find[inValue][find].RaceStatus?.isSuc != undefined){
            if(FindBlockAndSuccess.find[inValue][find].RaceStatus.isSuc){
                var worker = FindBlockAndSuccess["find"][inValue][find]["Worker"]
                tmpList.push(worker);
                if(!winWorkers.includes(worker)){
                    winWorkers.push(worker);
                }
            }

        }

    }

    for(var worker in winWorkers){
        var count = tmpList.filter(element => winWorkers[worker] === element ).length;
        data.push(count);
    }



    var chart = new Chart(raceWinMachinesChart,{
        type: 'bar',
        data: {
            labels: winWorkers,
            datasets: [
            {
                label: 'Count',
                data: data,
            }
            ]
        },
    });

}

function getStruDetail(isInit){
    $.ajax({
        url: 'http://'+ip_addr+':'+web_port+'/api/struData',
        type: 'POST', // HTTP 요청 방법 (GET, POST 등)
        contentType:'application/json',
        dataType: 'json', // 응답 데이터 유형 (JSON)
        data: JSON.stringify(workersDetail),
        async: !isInit,
        success: function(data) { // 요청 성공 시 콜백 함수
            struDetail = data.result;
            showWorkers();
            // 서버로부터 받은 JSON 데이터를 처리합니다.
        },
        error: function(xhr, status, error) { // 요청 실패 시 콜백 함수
            // 오류 처리
            errors.push("("+status+")"+error);
        }
    });
}

function checkDead(){
    deadList = [];
    deadServerTimes = [];
    $('#deadList').empty();
    for(var i =0; i < workers.length; i++){
        var currentTime = Date.now();
        // var difference = currentTime - workersDetail[workers[i]].timestamp;
        var difference = currentTime - workersDetail[workers[i]]['timestamp'];
        // serverTimes.push(workersDetail[workers[i]].timestamp);
        serverTimes.push(workersDetail[workers[i]]['timestamp']);
        var differenceMinutes = difference / (1000 * 60);
        // test
        // if (workers[i] == "node10"){
        //     deadList.push(workers[i]);
        //     var link = "http://"+ip_addr+":"+web_port+"/dashboard-detail/"+workers[i];
        //     $('#deadList').append("<li><a href='"+link+"'>"+workers[i]+"</a></li>");
        //     continue;
        // }
        if (differenceMinutes >= 15){
            deadList.push(workers[i]);
            var link = "http://"+ip_addr+":"+web_port+"/dashboard-detail/"+workers[i];
            $('#deadList').append("<li><a href='"+link+"'>"+workers[i]+"</a></li>")
        }else{

        }
    }
    $("#deadcountValue").text(deadList.length);
    $("#countValue").text(workers.length-deadList.length + " / " + workers.length);
}

function getCurrentBlockInfo(isInit){
    $.ajax({
        url: 'http://'+ip_addr+':'+web_port+'/api/getCurrentBlockInfo', // 서버 API 엔드포인트 URL
        type: 'GET', // HTTP 요청 방법 (GET, POST 등)
        dataType: 'json', // 응답 데이터 유형 (JSON)
        async: !isInit,
        success: function(data) { // 요청 성공 시 콜백 함수
            // 서버로부터 받은 JSON 데이터를 처리합니다.
            if (data.result.blockCount){
                $("#blockInfo").text(data.result.blockCount +" / "+parseFloat(data.result.overTime).toFixed(2)+" sec");
            }else{
                $("#blockInfo").text(data.result.overTime);
            }

        },
        error: function(xhr, status, error) { // 요청 실패 시 콜백 함수
            // 오류 처리
            errors.push("("+status+")"+error);
        }
    });
}

function getFindBlockAndSuccess(isInit){
    $.ajax({
        url: 'http://'+ip_addr+':'+web_port+'/api/getFindBlockAndSuccess', // 서버 API 엔드포인트 URL
        type: 'GET', // HTTP 요청 방법 (GET, POST 등)
        dataType: 'json', // 응답 데이터 유형 (JSON)
        async: true,
        success: function(data) { // 요청 성공 시 콜백 함수
            // 서버로부터 받은 JSON 데이터를 처리합니다.
            FindBlockAndSuccess = data.result;
            if (isInit){
                $("#eventInfoLoading").remove();
                $("#raceWinMachinesChartLoadgin").remove();
                $("#raceLost").text("RaceWin / Find");
                reFreshDropDownData();
            }else{
                reFreshDropDownData();
            }
        },
        error: function(xhr, status, error) { // 요청 실패 시 콜백 함수
            // 오류 처리
            errors.push("("+status+")"+error);
        }
    });
}

function goNodeDetail(worker){
    var arg = worker;
    window.open("http://"+ip_addr+":"+web_port+"/dashboard-detail/"+arg,'_blank');
}

function changeSelect(select){
    list = ["home","farms","set","logs"];
    for (var i =0; i < list.length; i++){
        if (list[i] == select){
            $("#nav_"+list[i]).addClass("navSelect");
            $("#"+list[i]+"Content").show();
        }else{
            $("#nav_"+list[i]).removeClass("navSelect");
            $("#"+list[i]+"Content").hide();
        }
    }
}

function countsTabChangeSelect(select){
    list = ["tfb","ts"];
    for (var i =0; i < list.length; i++){
        if (list[i] == select){
            $("#nav_"+list[i]).addClass("navSelect");
            $("#"+list[i]+"Content").show();
        }else{
            $("#nav_"+list[i]).removeClass("navSelect");
            $("#"+list[i]+"Content").hide();
        }
    }
}

function reFreshDropDownData(){
    if (FindBlockAndSuccess){
        changeDropDown("tfbContentTermBTN",tfbContentTermBTNFocus);
        changeDropDown("tsContentTermBTN",tsContentTermBTNFocus);
        changeDropDown("raceWinMachinesTermBTN",raceWinMachinesTermBTNFocus);
    }
}


function changeDropDown(contentID, value){
    $('#'+contentID).text(value);
    var inValue;
    switch(value){
        case "Total":
            inValue = "total";
            break;
        case "1 day":
            inValue = "1d";
            break;
        case "7 days":
            inValue = "7d";
            break;
        case "One month":
            inValue = "30d";
            break;
    }
    switch(contentID){
        case "tfbContentTermBTN":
            {
                tfbContentTermBTNFocus = value;
                for (var i = 0; i<FindBlockAndSuccess["find"][inValue].length; i++){
                    for(var key in FindBlockAndSuccess["find"][inValue][i]){
                        if (key == "info"){
                            $('#raceSuccess').text(FindBlockAndSuccess["find"][inValue][i][key]["SuccessCount"] + " / " + FindBlockAndSuccess["find"][inValue][i][key]["FindCount"]);
                        }

                    }
                }
            }
        break;
        case "tsContentTermBTN":
            {
                tsContentTermBTNFocus = value;
                $('#tsCount').text(FindBlockAndSuccess["complete"][inValue]['count']);
                $('#tsTime').text('AVG ' +FindBlockAndSuccess["complete"][inValue]['avg']+ ' sec');

            }
        break;
        case "raceWinMachinesTermBTN":
            {
                raceWinMachinesTermBTNFocus = value;
                drawRaceWinMachinesChart(inValue);
            }
        break;
    }
}



function showWorkers(){
    var tmp;
    for(var farm in struDetail){
        $("#farm_"+farm+"_container").remove();
        tmp =   "<div class='contentContainer farm' id='farm_"+farm+"_container'>"+
                    "<p class='contentName'>"+
                        "<ion-icon name='cloud-outline' class='contentIcon'></ion-icon>"+
                         farm+
                    "</p>"+
                "</div>"
        $('#farmsContent').append(tmp);

        for(var group in struDetail[farm]){

            tmp =   "<div class='contentContainer group' id='farm_"+farm+"_group_"+group+"container'>"+
                        "<p class='contentName'>"+
                            "<ion-icon name='file-tray-stacked-outline' class='contentIcon'></ion-icon>"+
                             group+
                        "</p>"+
                    "</div>"

            $("#farm_"+farm+"_container").append(tmp);

            for(var node in struDetail[farm][group]){
                var nodeName = struDetail[farm][group][node];
                tmp = "<button class=\"btnNode\" id=\"nodeBtn_"+farm+"_"+nodeName+"\" onclick=\"goNodeDetail('"+farm+"_"+nodeName+"')\" title=\""+nodeName+"\">"+nodeName+"</button>"
                $("#farm_"+farm+"_group_"+group+"container").append(tmp);
                if (deadList.includes(farm+"_"+nodeName)){
                    $("#nodeBtn_"+farm+"_"+nodeName).css("background-color", "lightcoral");
                }
            }
        }
    }

}

function getLogs(isInit){
    if (isInit){
        url = 'http://'+ip_addr+':'+web_port+'/api/getEventLogs'
    }else{
        url = 'http://'+ip_addr+':'+web_port+'/api/getEventLogs/'+logLastTime;
    }
    $.ajax({
        url: url, // 서버 API 엔드포인트 URL
        type: 'GET', // HTTP 요청 방법 (GET, POST 등)
        dataType: 'json', // 응답 데이터 유형 (JSON)
        async: true,
        success: function(data) { // 요청 성공 시 콜백 함수
            // 서버로부터 받은 JSON 데이터를 처리합니다.
            if (isInit){
                $("#logLoading").remove();
            }
            if (data.lastTime != null){
                logData = data;
                showLogs(isInit);
                logLastTime = data.lastTime;
            }

        },
        error: function(xhr, status, error) { // 요청 실패 시 콜백 함수
            // 오류 처리
            errors.push("("+status+")"+error);
        }
    });
}

function showLogs(isInit){
    if(logData["lastTime"]!=null){
        logLastTime = logData["lastTime"];
        logData['result'].reverse();
        for(var msg in logData['result']){
            if(msg == 0 && isInit){
                $("#logContent").append("<p class='text-break lh-sm' id='log_focus"+logFocus+"'>"+logData['result'][msg]+"</p>");
            }else{
                preFocus = logFocus -1;
                $("#log_focus"+preFocus).prepend("<p class='text-break lh-sm' id='log_focus"+logFocus+"'>"+logData['result'][msg]+"</p>");
            }
            logFocus+=1;
        }
    }
}