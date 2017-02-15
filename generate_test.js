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
			.option("-l, --lines", "Number of lines to generate.", parseInt)
			.option("-o, --out", "Name of output file.")
			.parse(process.argv);
		var ws = fs.createWriteStream(options.out || "test.txt");
		ws.once('error', (err) => { throw (err); });
		var name, age;
		var lines = options.lines || 50;
		var max_age = Math.floor(lines/10);
		for(let i = 0; i < lines; i++){
			name = names[getRandomInt(0, names.length - 1)];
			age = getRandomInt(1, max_age);
			ws.write(i+". " + name + ", " + age + "\n");
		}
		ws.end();
	} catch(ex){
		console.log("Unable to write test file. " + ex);
	}
	return;
})();