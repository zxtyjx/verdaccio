'use strict';

const fs = require('fs');
const Logger = require('./logger');
const request = require('sync-request');
const user = 'root';
const pass = 'root';
const database = 'verdaccio';
const domain = '127.0.0.1:5984';
console.log('\tCloudant User: root\n');
const url = `http://${user}:${pass}@${domain}`;

function LocalData(path) {
	let self = Object.create(LocalData.prototype);
	self.path = path;

	// backwards compat for a period of time....
	try {
		// migrate internal database and sync with couchdb collection
		let tmpData= JSON.parse(fs.readFileSync(self.path, 'utf8'));
		fs.unlinkSync(self.path); 
		// delete it so this part does not get called anymore
		// post version
		tmpData._id = 'localdatacache';
		request('POST', `${url}/${database}`, {
			json: tmpData,
		});
		self.data = tmpData;
		self.logger = Logger.logger.child({sub: 'fs'});
		return self;
	} catch(err) {
		// console.error('error');
		// self.logger.error('error ');
	}
	// end of backwards compat
	self.data = _getLocalDataCache();
	self.logger = Logger.logger.child({sub: 'fs'});
	return self;
}

LocalData.prototype.add = function(name) {
	let self = this;
	self.logger.info( {name: name}, 'add @{name}');
	this.data = _getLocalDataCache();
	if (this.data.list.indexOf(name) === -1) {
		this.data.list.push(name);
		this.sync();
	}
};

LocalData.prototype.remove = function(name) {
	let self = this;
	self.logger.info( {name: name}, 'remove @{name}');
	this.data = _getLocalDataCache();
	let i = this.data.list.indexOf(name);
	if (i !== -1) {
		this.data.list.splice(i, 1);
	}
	this.sync();
};

LocalData.prototype.get = function() {
	let self = this;
	self.logger.info( {data: this.data}, 'get @{data}');
	this.data = _getLocalDataCache();
	return this.data.list;
};

LocalData.prototype.sync = function() {
	let self = this;
	self.logger.info( {data: this.data}, 'sync @{data}');
	let revision = this.data.rev;
	delete this.data.rev;
	if (revision) {
	 // update version
	 request('PUT', `${url}/${database}/localdatacache?rev=${revision}`, {
				'json': this.data,
					'headers': {
					'Content-Type': 'application/json',
				},
		});
	} else {
		// create version
		this.data._id = 'localdatacache';
		request('POST', `${url}/${database}`, {
				'json': this.data,
					'headers': {
					'Content-Type': 'application/json',
				},
		});
	}
};

function _getLocalDataCache() {
	 let res = request('GET', `${url}/${database}/localdatacache`, {});
		if(res.statusCode > 200) {
			return {list: []};
		} else {
			let localData = JSON.parse(res.getBody('utf8') );
			return localData;
		}
}

module.exports = LocalData;
