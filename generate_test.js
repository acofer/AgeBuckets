/* jslint esversion:6 */
(function(){
	"use strict";

	const fs = require("fs");
	const options = require('commander');

	function getRandomInt(min, max) {
		min = Math.ceil(min);
		max = Math.floor(max);
		return Math.floor(Math.random() * (max - min)) + min;
	}

	const names = [
		"Mal",
		"Wash",
		"Zoë",
		"Shepherd",
		"River",
		"Jayne",
		"Kaylee",
		"Inara",
		"Simon",
	];

	try{
		options
			.version("0.0.1")
			.option("-l, --lines <n>", "Number of lines to generate.", parseInt)
			.option("-o, --out", "Name of output file.")
			.parse(process.argv);
		var ws = fs.createWriteStream(options.out || "test.txt");
		ws.once('error', (err) => { throw (err); });
		var name, age;
		var lines = options.lines || 50;
		var max_age = Math.min(Math.floor(lines/10) , 20);
		let i = 0, line;
		var write = function(){
			var ok = true;
			do {
				i++;
				name = names[getRandomInt(0, names.length - 1)];
				age = getRandomInt(1, max_age);
				line = i + ". " + name + ", " + age + "\n";
				ok = ws.write(line, "utf8");
				if (i === 0){ ws.end(); }
			} while (i < lines && ok);
			if (i < lines){
				ws.once('drain', write);
			}
		};
		write();
	} catch(ex){
		console.log("Unable to write test file. " + ex);
	}
	return;
})();