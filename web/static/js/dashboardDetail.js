let ip_addr;
let web_port;
let nodeID;
let errors = [];
let nodeDetail;
let blockInfo;
let cpuInfo;
let memoryInfo;
let minerInfo;
let minerStatus;
let serverCurrentTime;
let charts = {};
let staticCount = 0;


$(document).ready(function (){
    ip_addr = $("#ip_addr").text();
    web_port = $("#web_port").text();
    nodeID = $('.header_name').text();
    // ip_addr = "127.0.0.1"
    // web_port = 2648
    // nodeID = "node10"
    setResize();
    init();
    hideLoadingPanel();
    checkErrors();
    reloadDatas();
    setInterval(reloadDatas, 2000);
    setInterval(reloadBigDatas, 1000000);
})

function init(){
    $(".header_name").text(nodeID+" Details");
    getDetail(true);
    showTime();
    parseResources();
    showBlockAndMiner(true);
    showResources(true);
    staticCount+=1;
}

function reloadDatas(){
    getDetail(false);
    showTime();
    parseResources();
    showBlockAndMiner(false);
    showResources(false);
    staticCount+=1;
}

function reloadBigDatas(){

}


window.addEventListener('resize', setResize);

function setResize(){
    var height = $('.staticArea').height();
    $(".webContent").css('margin-top',height);
    chartResize();
}

function chartResize(){
    var ids = $('canvas').map(function() {
        return this.id;
    }).get().filter(id => id !== '');

      // 결과를 콘솔에 출력합니다.
    for (var canvasId in ids){
        var chartStatus = Chart.getChart(charts[canvasId]);
        if (chartStatus != undefined){
            chartStatus.destroy();
        }
    }
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

// function getStruDetail(isInit){
//     $.ajax({
//         url: 'http://'+ip_addr+':'+web_port+'/api/struData',
//         type: 'POST', // HTTP 요청 방법 (GET, POST 등)
//         contentType:'application/json',
//         dataType: 'json', // 응답 데이터 유형 (JSON)
//         data: JSON.stringify(workersDetail),
//         async: !isInit,
//         success: function(data) { // 요청 성공 시 콜백 함수
//             struDetail = data.result;
//             showWorkers();
//             // 서버로부터 받은 JSON 데이터를 처리합니다.
//         },
//         error: function(xhr, status, error) { // 요청 실패 시 콜백 함수
//             // 오류 처리
//             errors.push("("+status+")"+error);
//         }
//     });
// }

function getDetail(isInit){
    $.ajax({
        url: 'http://'+ip_addr+':2648/api/getWorkerDetails/' + nodeID,
        type: 'GET', // HTTP 요청 방법 (GET, POST 등)
        dataType: 'json', // 응답 데이터 유형 (JSON)
        async: !isInit,
        success: function(data) { // 요청 성공 시 콜백 함수
            nodeDetail = data.result;
            parseResources();
        },
        error: function(xhr, status, error) { // 요청 실패 시 콜백 함수
            // 오류 처리
            console.error('AJAX request failed: ', status, error);
        }
    });
}

function parseResources(){
    for (var column in nodeDetail[nodeID]){
        switch(column){
            case "Block Info":
                {
                    blockInfo = nodeDetail[nodeID][column];
                }
            break;
            case "CPU Info":
                {
                    cpuInfo = nodeDetail[nodeID][column];
                }
            break;
            case "Memory Info":
                {
                    memoryInfo = nodeDetail[nodeID][column];
                }
            break;
            case "Miner Info":
                {
                    minerInfo = nodeDetail[nodeID][column];
                }
            break;
            case "Miner Status":
                {
                    minerStatus = nodeDetail[nodeID][column];
                }
        }
    }
}

function showBlockAndMiner(isInit){
    if(isInit){
        // NodeInfo
        tmp = "<div class='contentContainer' id='workerInfo'>"+
                    "<p class='contentName'>"+
                    "<ion-icon name='list-outline' class='contentIcon'></ion-icon>"+
                        "WorkerInfo" +
                    "</p>"+
                    "<p class='countValue bnm' id='node_farm'> Farm: "+minerInfo["Farm"]+"</p>"+
                    "<p class='countValue bnm' id='node_group'> Group: "+minerInfo["Group"]+"</p>"+
                    "<p class='countValue bnm' id='node_mode'> MinerMode: "+minerInfo["Miner Mode"]+"</p>"+
                    "<p class='countValue bnm' id='node_ver'> Version: "+minerInfo["Version"]+"</p>"+
                    "<p class='countValue bnm' id='node_wor'> WorkerName: "+minerInfo["Worker"]+"</p>"+
            "</div>"
        $("#blockContent").append(tmp);

        // BlockInfo
        tmp = "<div class='contentContainer' id='blockInfo'>"+
                    "<p class='contentName'>"+
                    "<ion-icon name='list-outline' class='contentIcon'></ion-icon>"+
                        "BlockInfo" +
                    "</p>"+
                    "<p class='countValue bnm' id='block_Block_nBits'> Block nBits: "+blockInfo["Block nBits"]+"</p>"+
                    "<p class='countValue bnm' id='block_BlockTime'> BlockTime: "+blockInfo["BlockTime"]+"</p>"+
                    "<p class='countValue bnm' id='block_CurruentBlock'> CurrentBlock: "+blockInfo["CurruentBlock"]+"</p>"+
            "</div>"
        $("#blockContent").append(tmp);

        // MinerInfo
        tmp = "<div class='contentContainer' id='minerInfo'>"+
                    "<p class='contentName'>"+
                    "<ion-icon name='list-outline' class='contentIcon'></ion-icon>"+
                        "MinerInfo" +
                    "</p>"+
                    "<p class='countValue bnm' id='miner_cado_url'> Block nBits: "+minerInfo["CADO-NFS Server URL"]+"</p>"+
                    "<p class='countValue bnm' id='miner_cpuECM_ip'> CPU ECM Server IP Address: "+minerInfo["CPU ECM Server IP Address"]+"</p>"+
                    "<p class='countValue bnm' id='miner_cpuECM_port'> CPU ECM Server Port Number: "+minerInfo["CPU ECM Server Port Number"]+"</p>"+
                    "<p class='countValue bnm' id='miner_cuda_ip'> CUDA ECM Server IP Address: "+minerInfo["CUDA ECM Server IP Address"]+"</p>"+
            "</div>"
        $("#blockContent").append(tmp);

        // MinerStatus
        tmp = "<div class='contentContainer' id='minerStatus'>"+
                    "<p class='contentName'>"+
                    "<ion-icon name='list-outline' class='contentIcon'></ion-icon>"+
                        "MinerStatus" +
                    "</p>"+
                    "<p class='countValue bnm' id='stat_cado'> CADO Client Processes: "+minerStatus["CADO Client Processes"]+"</p>"+
                    "<p class='countValue bnm' id='stat_cado_m_url'> CADO Master URL: "+minerStatus["CADO Master URL"]+"</p>"+
            "</div>"
        $("#blockContent").append(tmp);
    }else{
        //NodeInfo
        $("#node_farm").text("Farm: "+minerInfo["Farm"]);
        $("#node_group").text("Group: "+minerInfo["Group"]);
        $("#node_mode").text("MinerMode: "+minerInfo["Miner Mode"]);
        $("#node_ver").text("Version: "+minerInfo["Version"]);
        $("#node_wor").text("WorkerName: "+minerInfo["Worker"]);
        //BlockInfo
        $("#block_Block_nBits").text("Block nBits: "+blockInfo["Block nBits"]);
        $("#block_BlockTime").text("BlockTime: "+blockInfo["BlockTime"]);
        $("#block_CurruentBlock").text("CurrentBlock: "+blockInfo["CurruentBlock"]);
        //MinerInfo
        $("#miner_cado_url").text("Block nBits: "+minerInfo["CADO-NFS Server URL"]);
        $("#miner_cpuECM_ip").text("CPU ECM Server IP Address: "+minerInfo["CPU ECM Server IP Address"]);
        $("#miner_cpuECM_port").text("CPU ECM Server Port Number: "+minerInfo["CPU ECM Server Port Number"]);
        $("#miner_cuda_ip").text("CUDA ECM Server IP Address: "+minerInfo["CUDA ECM Server IP Address"]);
        //MinerStatus
        $("#stat_cado").text("CADO Client Processes: "+minerStatus["CADO Client Processes"]);
        $("#stat_cado_m_url").text("CADO Master URL: "+minerStatus["CADO Master URL"]);
    }
}

function showResources(isInit){
    // $("#cpuFrequency").remove();

    if(isInit){
        var tmp;
        // CPU Frequency
        tmp = "<div class='contentContainer' id='cpuFrequency'>"+
                    "<p class='contentName'>"+
                    "<ion-icon name='hardware-chip-outline' class='contentIcon'></ion-icon>"+
                        "CPU Frequency" +
                    "</p>"+
                    "<p class='countValue cf' id='Current_Frequency_Mhz'> Current "+cpuInfo["Current Frequency Mhz"]+" Mhz</p>"+
                    "<p class='countValue cf' id='Max_Frequency_Mhz'> Max "+cpuInfo["Max Frequency Mhz"]+" Mhz</p>"+
                    "<p class='countValue cf' id='Min_Frequency_Mhz'> Min "+cpuInfo["Min Frequency Mhz"]+" Mhz</p>"+
            "</div>"
        $("#resourcesContent").append(tmp);
        delete cpuInfo["Current Frequency Mhz"];
        delete cpuInfo["Max Frequency Mhz"];
        delete cpuInfo["Min Frequency Mhz"];

        // CPU TotalUsage
        tmp = "<div class='contentContainer' id='cpuTotalUsage'>"+
                "<p class='contentName'>"+
                "<ion-icon name='hardware-chip-outline' class='contentIcon'></ion-icon>"+
                    "CPU TotalUsage" +
                "</p>"+
                "<div id='totalCpuUsageChartArea'>"+
                    "<canvas id='totalCpuUsageChart'></canvas>"+
                "</div>"+
                "<p class='countValue cpuVal' id='tcup'> Current "+cpuInfo["Total Core Usage Percent"]+" %</p>"+
        "</div>"
        $("#resourcesContent").append(tmp);
        drawResourcesChart("totalCpuUsageChart", "Total Core Usage Percent", isInit, cpuInfo["Total Core Usage Percent"]);
        delete cpuInfo["Total Core Usage Percent"];

        // Memory
        tmp = "<div class='contentContainer' id='memFrequency'>"+
                    "<p class='contentName'>"+
                    "<ion-icon name='hardware-chip-outline' class='contentIcon'></ion-icon>"+
                        "Memory Info" +
                    "</p>"+
                    "<p class='countValue cf' id='mem_Available'> Available "+memoryInfo["Available GB"]+" GB</p>"+
                    "<p class='countValue cf' id='mem_Total'> Total "+memoryInfo["Total GB"]+" GB</p>"+
                    "<p class='countValue cf' id='mem_Percent'> Percent "+memoryInfo["Used Percent"]+" %</p>"+
            "</div>"
        $("#resourcesContent").append(tmp);

        //CPUs
        for(var column in cpuInfo){
            var stripColumn = column.replaceAll(' ', '');
            tmp = "<div class='contentContainer' id='cpuTotalUsage'>"+
                "<p class='contentName'>"+
                "<ion-icon name='hardware-chip-outline' class='contentIcon'></ion-icon>"+
                    stripColumn +
                "</p>"+
                "<div id='"+stripColumn+"ChartArea'>"+
                    "<canvas id='"+stripColumn+"Chart'></canvas>"+
                "</div>"+
                "<p class='countValue cpuVal' id='"+stripColumn+"Text'> Current "+cpuInfo[column]+" %</p>"+
            "</div>"
            $("#resourcesContent").append(tmp);
            drawResourcesChart(stripColumn+"Chart", stripColumn, isInit, cpuInfo[column]);
        }

    }else{
        // CPU Frequency
        $("#Current_Frequency_Mhz").text("Current "+cpuInfo["Current Frequency Mhz"]+" Mhz");
        $("#Max_Frequency_Mhz").text("Max "+cpuInfo["Max Frequency Mhz"]+" Mhz");
        $("#Min_Frequency_Mhz").text("Min "+cpuInfo["Min Frequency Mhz"]+" Mhz");
        delete cpuInfo["Current Frequency Mhz"];
        delete cpuInfo["Max Frequency Mhz"];
        delete cpuInfo["Min Frequency Mhz"];

        // CPU TotalUsage
        drawResourcesChart("totalCpuUsageChart", "Total Core Usage Percent", isInit, cpuInfo["Total Core Usage Percent"]);
        $("#tcup").text(cpuInfo["Total Core Usage Percent"]+ " %");
        delete cpuInfo["Total Core Usage Percent"];

        // Memory
        $("#mem_Available").text("Available "+memoryInfo["Available GB"]+" GB");
        $("#mem_Total").text("Total "+memoryInfo["Total GB"]+" GB");
        $("#mem_Percent").text("Percent "+memoryInfo["Used Percent"]+" %");

        //CPUs
        for(var column in cpuInfo){
            var stripColumn = column.replaceAll(' ', '');
            drawResourcesChart(stripColumn+"Chart", stripColumn, isInit, cpuInfo[column]);
            $("#"+stripColumn+"Text").text(cpuInfo[column]+ " %");
        }
    }


}

function drawResourcesChart(canvasDocId, chartName, isInit, data){
    var canvasENT = $("#"+canvasDocId);

    var chartStatus = Chart.getChart(charts["canvasDocId"]);
    if (chartStatus != undefined){
        chartStatus.destroy();
    }
    if (isInit){
        charts[chartName] = new Chart(canvasENT,{
            type: 'line',
            data: {
                labels: [],
                datasets: [
                {
                    label: "",
                    data: [],
                }
                ]
            },
            options: {
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    x: {
                        type: 'linear',
                        position: 'bottom',
                        display: false
                    },
                    // y: {
                    //     min: 0,
                    //     max: 100
                    // }

                }
            },
        });

        addChart(chartName, data);
    }else if(!isInit){
        addChart(chartName, data);
    }

}

function addChart(chartName, data){
    if(charts[chartName].data.labels.length > 10){
        charts[chartName].data.labels.shift();
        charts[chartName].data.datasets.forEach((dataset) => {
            dataset.data.shift(); // 가장 오래된 데이터 포인트 제거
        });
    }
    charts[chartName].data.labels.push(staticCount);
        charts[chartName].data.datasets.forEach((dataset) => {
            dataset.data.push(data);
        });
        charts[chartName].update();
}

function changeSelect(select){
    list = ["block","resources"];
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

function getTime(time){
    if (time == null){
        var date = new Date();
    }else{
        var date = new Date(time);
    }

    var yyyy = date.getFullYear();
    var mm = date.getMonth();
    var dd = date.getDate();

    var HH = date.getHours();
    var MM = date.getMinutes();
    var ss = date.getSeconds();

    return yyyy + "." + mm + "." + dd + " / " + HH + ":" + MM + "." + ss;
}

function showTime(){
    $("#nowTime").text("Time: "+getTime(null));
    $("#serverTime").text("ServerTime: "+getTime(nodeDetail[nodeID]['timestamp']));
}