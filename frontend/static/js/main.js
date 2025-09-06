// 获取设备ID
function getDeviceId() {
  return document.getElementById("deviceId").value || "root.example.exampledev";
}

// 显示结果
function showResult(elementId, message, isSuccess) {
  const element = document.getElementById(elementId);
  element.textContent = message;
  element.className = "result " + (isSuccess ? "success" : "error");
}

// 显示加载状态
function showLoading(elementId) {
  const element = document.getElementById(elementId);
  element.textContent = "处理中...";
  element.className = "result loading";
}

// 导入CSV文件
function importCSV() {
  const csvFile = document.getElementById("csvFile").value;
  const deviceId = getDeviceId();

  if (!csvFile) {
    showResult("importResult", "请输入CSV文件路径", false);
    return;
  }

  showLoading("importResult");

  fetch("/import", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: `csvFile=${encodeURIComponent(csvFile)}&deviceId=${encodeURIComponent(deviceId)}`,
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.success) {
        showResult("importResult", data.message, true);
      } else {
        showResult("importResult", data.error, false);
      }
    })
    .catch((error) => {
      showResult("importResult", "请求失败: " + error.message, false);
    });
}

// 获取统计数据
function getStatistic() {
  const deviceId = getDeviceId();
  showLoading("analysisResult");

  fetch(`/statistic?deviceId=${encodeURIComponent(deviceId)}`)
    .then((response) => response.json())
    .then((data) => {
      if (data.success) {
        showResult("analysisResult", data.data, true);
      } else {
        showResult("analysisResult", data.error, false);
      }
    })
    .catch((error) => {
      showResult("analysisResult", "请求失败: " + error.message, false);
    });
}

// 获取相关性数据
function getCorrelation() {
  const deviceId = getDeviceId();
  showLoading("analysisResult");

  fetch(`/correlation?deviceId=${encodeURIComponent(deviceId)}`)
    .then((response) => response.json())
    .then((data) => {
      if (data.success) {
        showResult("analysisResult", data.data, true);
      } else {
        showResult("analysisResult", data.error, false);
      }
    })
    .catch((error) => {
      showResult("analysisResult", "请求失败: " + error.message, false);
    });
}

// 获取图表数据
function getGraph() {
  const deviceId = getDeviceId();
  showLoading("analysisResult");

  fetch(`/graph?deviceId=${encodeURIComponent(deviceId)}`)
    .then((response) => response.json())
    .then((data) => {
      if (data.success) {
        // 解析返回的数据，提取文件名并创建链接
        const lines = data.data.split("\n");
        let resultHtml = "<div>图表生成成功！</div>";
        resultHtml += "<div>访问图表文件：</div>";
        resultHtml += "<ul>";

        // 为每个生成的文件创建链接
        lines.forEach((line) => {
          if (line.includes(".html")) {
            const fileName = line.split(": ")[1];
            if (fileName) {
              resultHtml += `<li><a href="/static/graphs/${fileName}" target="_blank">${fileName}</a></li>`;
            }
          }
        });

        resultHtml +=
          '<li><a href="/static/graphs/" target="_blank">查看所有图表</a></li>';
        resultHtml += "</ul>";
        // 设置结果区域为HTML内容
        const resultElement = document.getElementById("analysisResult");
        resultElement.innerHTML = resultHtml;
        resultElement.className = "result success";
      } else {
        showResult("analysisResult", data.error, false);
      }
    })
    .catch((error) => {
      showResult("analysisResult", "请求失败: " + error.message, false);
    });
}

// 获取条件分析数据
function getCondition() {
  const deviceId = getDeviceId();
  showLoading("analysisResult");

  fetch(`/condition?deviceId=${encodeURIComponent(deviceId)}`)
    .then((response) => response.json())
    .then((data) => {
      if (data.success) {
        showResult("analysisResult", data.data, true);
      } else {
        showResult("analysisResult", data.error, false);
      }
    })
    .catch((error) => {
      showResult("analysisResult", "请求失败: " + error.message, false);
    });
}
