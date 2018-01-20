var tasksGraphData = []

function loadTasksGraph() {
    var tasksGraphRootG = document.getElementById("tasksGraphRootG");
    if (tasksGraphRootG != null) { tasksGraphRootG.parentNode.removeChild(tasksGraphRootG); }
    
    // The number of series.
    var seriesNumber = 2,
    // The number of values per series.
    seriesValue = 60,
    // The colors used in the graph.
    colors = [d3.schemeCategory20c[0], d3.schemeCategory20c[4]];
    // Calc width and height in the graph.
    document.getElementById("tasksGraph").setAttribute("width", document.getElementById("tasksGraphContain").offsetWidth - 30);
    document.getElementById("tasksGraph").setAttribute("height", document.getElementById("tasksGraphContain").offsetWidth / 2 + 25);

    // The xz array has seriesValue elements, representing the x-values shared by all series.
    // The yz array has seriesNumber elements, representing the y-values of each of the series.
    // Each yz[i] is an array of seriesValue non-negative numbers representing a y-value for xz[i].
    // The y01z array has the same structure as yz, but with stacked [y₀, y₁] instead of y.
    var xz = d3.range(seriesValue),
    x00z = xz.map(function(x) { return x; }),
    // yz = d3.range(seriesNumber).map(function() { return bumps(seriesValue); }),
    yz = tasksGraphData,
    y01z = d3.stack().keys(d3.range(seriesNumber))(d3.transpose(yz)),
    yMax = d3.max(yz, function(y) { return d3.max(y); }),
    y1Max = d3.max(y01z, function(y) { return d3.max(y, function(d) { return d[1]; }); });

    // Generate a main g tab in svg, hold all graph items in it.
    var svg = d3.select("#tasksGraph"),
    margin = {top: 10, right: 10, bottom: 50, left: 10},
    width = +svg.attr("width") - margin.left - margin.right,
    height = +svg.attr("height") - margin.top - margin.bottom,
    g = svg.append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
    .attr("id", "tasksGraphRootG");

    // Create x axis.
    var xAxis = d3.scaleBand()
    .domain(xz)
    .rangeRound([30, width])
    .padding(0.08);

    // Create y axis.
    var yAxis = d3.scaleLinear()
    .domain([0, y1Max])
    .range([height, 0]);

    // Set color.
    var color = d3.scaleOrdinal()
    .domain(d3.range(seriesNumber))
    .range(colors);

    // Fill the color setted.
    var series = g.selectAll(".series")
    .data(y01z)
    .enter().append("g")
    .attr("fill", function(d, i) { return color(i); });

    // Draw rect in g tabs.
    var rect = series.selectAll("rect")
    .data(function(d) { return d; })
    .enter().append("rect")
    .attr("x", function(d, i) { return xAxis(i); })
    .attr("y", height)
    .attr("width", xAxis.bandwidth())
    .attr("height", 0);

    // Transition the rect.
    rect.transition()
    .delay(function(d, i) { return i * 10; })
    .attr("y", function(d) { return yAxis(d[1]); })
    .attr("height", function(d) { return yAxis(d[0]) - yAxis(d[1]); });

    // Create x axis tags.
    var xAxisTags = d3.scaleBand()
    .domain(x00z)
    .rangeRound([0, width - 30])
    .padding(0.08);

    // Draw x axis at the bottom of the graph.
    g.append("g")
    .attr("class", "axis axis--x tasksgraph-axis--x")
    .attr("transform", "translate(30," + height + ")")
    .call(d3.axisBottom(xAxisTags)
          .tickSize(0)
          .tickPadding(6));

    // Move x axis tags to left margin
    d3.select(".tasksgraph-axis--x")
    .selectAll("text")
    .attr("x", -xAxis.bandwidth()/2);

    // Add 24:00 x axis tag at the right.
    var node = d3.select(".tasksgraph-axis--x").selectAll("g")._groups[0][59].cloneNode(true),
    node2 = d3.select(".tasksgraph-axis--x").selectAll("g")._groups[0][58].cloneNode(true),
    transformValue = node.getAttribute("transform"),
    transformValue2 = node2.getAttribute("transform"),
    translateValue = transformValue.substring(transformValue.indexOf("(") + 1, transformValue.indexOf(",")),
    translateValue2 = transformValue2.substring(transformValue2.indexOf("(") + 1, transformValue2.indexOf(","));
    translateValue = parseFloat(translateValue) + parseFloat(translateValue) - parseFloat(translateValue2);
    transformValue = transformValue.substring(0, transformValue.indexOf("(") + 1) + translateValue + transformValue.substring(transformValue.indexOf(","), transformValue.length);
    node.setAttribute("transform", transformValue);
    node.children[1].innerHTML = "60";
    d3.select(".tasksgraph-axis--x")._groups[0][0].appendChild(node);

    // Change the default status to grouped.
    //drawYAxis();
    d3.selectAll("input")
    .on("change", transitionGroupedChanged);

    // Draw legend at the bottom.
    var legendRectWidth = 25,
    legendRectPadding = 5,
    legendRectHeight = 20,
    legendGMargin = 15,
    legendData = [
        {
            value: "Success Objects",
            width: width / 2 - (180 + legendRectWidth + legendRectPadding + legendGMargin) / 2 - 40
        },
        {
            value: "Failure Objects",
            width: width / 2 + (180 + legendRectWidth + legendRectPadding + legendGMargin) / 2 - 40
        }
    ];

    g.append("g")
    .attr("class", "legendGroup")
    .attr("transform", "translate(0," + (height + 24) + ")")

    var legendGroups = svg.selectAll('.legendGroup').selectAll('g')
    .data(legendData)
    .enter()
    .append('g')
    .attr(
          'transform', function(d, i) {
                var x = d.width;
                return 'translate(' + x + ',' + (legendRectHeight / 2) + ')'
            }
          )

    legendGroups.append('rect')
    .attr('width', legendRectWidth)
    .attr('height', legendRectHeight)
    .attr('x', 0)
    .attr('y', -(legendRectHeight/2))
    .attr('rx', 5)
    .attr('ry', 5)
    .attr('fill', function(d, i) {
          return colors[i];
          })

    legendGroups.append('text')
    .attr('x', legendRectWidth + legendRectPadding)
    .attr('y', 5)
    .attr('alignment-baseline', 'middle')
    .attr('fill', 'black')
    .attr('style', 'font-size: 12px')
    .text(function(d, i) {
            return d.value;
          })

    var timeout = d3.timeout(function() {
                             d3.select("input[value=\"grouped\"]")
                             .property("checked", true)
                             .dispatch("change");
                             }, 2000);

    function transitionGroupedChanged() {
        timeout.stop();
        if (this.value === "grouped") transitionGrouped();
        else transitionStacked();
    }

    function transitionGrouped() {
        yAxis.domain([0, yMax]);
        
        rect.transition()
        .duration(500)
        .delay(function(d, i) { return i * 10; })
        .attr("x", function(d, i) { return xAxis(i) + xAxis.bandwidth() / seriesNumber * this.parentNode.__data__.key; })
        .attr("width", xAxis.bandwidth() / seriesNumber)
        .transition()
        .attr("y", function(d) { return yAxis(d[1] - d[0]); })
        .attr("height", function(d) { return yAxis(0) - yAxis(d[1] - d[0]); });
        
        drawYAxis();
    }

    function transitionStacked() {
        yAxis.domain([0, y1Max]);
        
        rect.transition()
        .duration(500)
        .delay(function(d, i) { return i * 10; })
        .attr("y", function(d) { return yAxis(d[1]); })
        .attr("height", function(d) { return yAxis(d[0]) - yAxis(d[1]); })
        .transition()
        .attr("x", function(d, i) { return xAxis(i); })
        .attr("width", xAxis.bandwidth());

        drawYAxis();
    }

    // Draw y axis at the left of the graph.
    function drawYAxis() {
        g.selectAll(".tasksgraph-axis--y").remove();
        
        g.append("g")
        .attr("class", "axis axis--y tasksgraph-axis--y")
        .attr("transform", "translate(30,0)")
        .call(d3.axisLeft(yAxis)
              .tickSize(6)
              .tickPadding(6));
    }
}

// Get Data From API Endpoint.
function getDataFromAPIEndpoint() {
    if (window.XMLHttpRequest) {
        var obj = new XMLHttpRequest();
    } else if (window.ActiveXObject) {
        var obj = new ActiveXObject("Microsoft.XMLHTTP");
    }
    obj.open("GET", APIEndpoint + "tasksGraph", true);
    
    obj.onreadystatechange = function() {
        if (obj.readyState == 4 && (obj.status == 200 || obj.status == 304)) {
            data = eval("(" + obj.responseText + ")");
            
            tasksGraphData = [data.successObjects, data.failureObjects];
            console.log(tasksGraphData);
            loadTasksGraph();
        }
    };
    obj.send(null);
}

// Just For Test.
// Returns an array of seriesValue psuedorandom, smoothly-varying non-negative numbers.
// Inspired by Lee Byron’s test data generator.
// http://leebyron.com/streamgraph/
function bumps(value) {
    var values = [], i, j, w, x, y, z;
    
    // Initialize with uniform random values in [0.1, 0.2).
    for (i = 0; i < value; ++i) {
        values[i] = 0.1 + 0.1 * Math.random();
    }
    
    // Add five random bumps.
    for (j = 0; j < 5; ++j) {
        x = 1 / (0.1 + Math.random());
        y = 2 * Math.random() - 0.5;
        z = 10 / (0.1 + Math.random());
        for (i = 0; i < value; i++) {
            w = (i / value - y) * z;
            values[i] += x * Math.exp(-w * w);
        }
    }
    
    // Ensure all values are positive.
    for (i = 0; i < value; ++i) {
        values[i] = Math.max(0.01, values[i]);
    }
    
    // Map Float to Int.
    var minValue = 0.2
    for (i = 0; i < value; ++i) {
        minValue = Math.min(minValue, values[i]);
    }
    for (i = 0; i < value; ++i) {
        values[i] = Math.round(values[i] / minValue);
    }
    
    console.log(values);
    return values;
}

getDataFromAPIEndpoint()
