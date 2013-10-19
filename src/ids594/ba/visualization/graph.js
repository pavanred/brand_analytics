var w, h, fill, vis, data;

function run(jsonFile) {		
	initVisualization('#graph');
	d3.json(jsonFile, startVisualization);
}

function initVisualization(divId) {
	w = d3.select(divId)[0][0].offsetWidth;  //#graph
	h = d3.select(divId)[0][0].offsetHeight;
		   
	vis = d3.select(divId)
		.append("svg:svg")
		.attr("width", "100%")
		.attr("height", "100%")
		.attr("pointer-events", "all")
		.append("svg:g")
		.call(d3.behavior.zoom().on("zoom", redraw))
		.append("svg:g");
			
	vis.style("opacity", 1e-6)
		.transition()
		.duration(1000)
		.style("opacity", 1);			
}

function redraw() {
	vis.attr("transform",
		"translate(" + d3.event.translate + ")"
		+ "scale(" + d3.event.scale + ")");
}

function collide(node) {
	var nx1 = node.x,
	nx2 = node.x + node.width,
	ny1 = node.y,
	ny2 = node.y + node.height;
			
	return function(quad, x1, y1, x2, y2) {
		if (quad.point && (quad.point !== node)) {
			if (node.x <= quad.point.x) {
				var dist = quad.point.x - node.x; 
				if (dist < node.width) {
					node.px -= 5;
				}
			} else {
				var dist = node.x -quad.point.x; 
				if (dist < quad.point.width) {
					node.px += 5;
				}
			}
			if (node.y <= quad.point.y) {
				var dist = quad.point.y - node.y; 
				if (dist < node.height) {
					node.py -= 5;
				}
			} else {
				var dist = node.y -quad.point.y; 
				if (dist < quad.point.height) {
					node.py +=  5;
			}
		}
	}
	return x1 > nx2
			|| x2 < nx1
			|| y1 > ny2
			|| y2 < ny1;
	};
};

function startVisualization(json) {
	data = json; 
			   
	vis.selectAll("line.link").remove();
	vis.selectAll("ellipse.node").remove();
			   
	var force = d3.layout.force()
				.gravity(1)
				.charge(-100)
				.linkDistance(400)
				.nodes(json.nodes)
				.links(json.links)
				.size([w, h])
				.start();
			   
	var link = vis.selectAll("line.link")
			   .data(json.links)
			   .enter().append("svg:line")
			   .attr("class", "link")
			   .attr("x1", function(d) { return d.source.x; })
			   .attr("y1", function(d) { return d.source.y; })
			   .attr("x2", function(d) { return d.target.x; })
			   .attr("y2", function(d) { return d.target.y; })
			   .attr("rel",function(d) { return d.relation; });

			
	var node = vis.selectAll("ellipse.node")
			   .data(json.nodes)
			   .enter().append("svg:ellipse")
			   .attr("class", "node")
			   .attr("x", function(d) { return d.x; })
			   .attr("y", function(d) { return d.y; })
			   .attr("id", function(d) { return d.index; })
			   .attr("cx", 15)
			   .attr("cy", 15)
			   .attr("rx", 25)
			   .attr("ry", 15)
			   .style("fill", "#FFFFFF")
			   .call(force.drag);
			
	node.append("svg:title").text(function(d) { return d.uri; });
			   
	vis.selectAll("text")
		.data(json.nodes)
		.enter().append("svg:text")
		.attr("dx", 0)
		.attr("dy", 0)
		.attr("id", function(d) { return d.index + "_text"; })
		.text(function(d) { return d.label; });
			   
	node = vis.selectAll("ellipse.node, text");
			   
	vis.selectAll("ellipse.node").each(function(node) {
		var text = document.getElementById(this.id + "_text");
		var bBox = text.getBBox();
		this.setAttributeNS(null, "rx", (bBox.width + 15)/2);
		this.setAttributeNS(null, "ry", (bBox.height + 10)/2);
		text.setAttributeNS(null,"dx", -((bBox.width)/2));
		text.setAttributeNS(null,"dy", (bBox.height - 8)/2);
	});
			   
	var graph = d3.select('#graph')[0][0];
			
	force.on("tick", function() {
		var w = graph.offsetWidth;
		var h = graph.offsetHeight;
			
		force.size([w, h]);
				  
		var q = d3.geom.quadtree(data["nodes"]),
		i = 0,
		n = data["nodes"].length;
		l = data["links"].length;
			
		while (++i < n) {
			q.visit(collide(data["nodes"][i]));
		}
			
		link.attr("x1", function(d) { return d.source.x; })
			 .attr("y1", function(d) { return d.source.y; })
			 .attr("x2", function(d) { return d.target.x; })
			 .attr("y2", function(d) { return d.target.y; });
			
		node.attr("cx", function(d) { return d.x = Math.max(50, Math.min(w - 50,d.x)); })
			.attr("cy", function(d) { return d.y = Math.max(10, Math.min(h - 10,d.y)); })
			.attr("x", function(d) { return d.x = Math.max(50, Math.min(w - 50,d.x)); })
			.attr("y", function(d) { return d.y = Math.max(10, Math.min(h - 10,d.y)); });
	});		   
}