/* jslint esversion:6 */
(function(){
	"use strict";

	const fs = require("fs");
	const readline = require("readline");


	function bucketOut(buckets){
		for(let i in buckets){
			console.log(i + "," + buckets[i]);
		}
	}


	function processLine(line, buckets){
		// given an array of characters, process it as a line of input
		var sp = line.split(","), age;
		if (sp.length >= 2){
			age = sp[1].trim();
			if (!buckets[age]){ buckets[age] = 0; }
			buckets[age]++;
		}
	}


	try {
		var filename = process.argv[2];

		var stat = fs.statSync(filename);

		if (!stat || stat.size <= 0){
			console.log("Empty file.");
			return;
		}

		/* small file, regular approach: read file by line and tally counts */
		var rs = fs.createReadStream(filename, { encoding: "utf8" });
		rs.once("error", (err) => { throw (err); });
		var rl = readline.createInterface({
			input: rs
		});
		var sp, id, age, buckets = {};
		rl.on("line", (line)=>{
			processLine(line, buckets);
		});
		rl.on("close", ()=>{ bucketOut(buckets); });
		return 0;
	} catch(ex){
		console.log("Exception in execution: " + ex);
		return -1;
	}
})();