'use strict';

let MyStreams = require('./streams');
let Error = require('http-errors');
let fs = require('fs');

let HttpsAgent = require('agentkeepalive').HttpsAgent;
let myagent = new HttpsAgent({
		maxSockets: 50,
		maxKeepAliveRequests: 0,
		maxKeepAliveTime: 30000,
	});

let user = 'root';
let pass = 'root';

let url = 'http://root:root@127.0.0.1:5984';
let nano = require('nano')({
		'url': url,
		// 'requestDefaults': {'agent': myagent},
});

function NanoError(code) {
	let err = Error(code);
	err.code = code;
	return err;
}

module.exports.create_json = create_json;

function create_json(name, value, cb) {
	document_exists(value.name, function(err, headers) {
			if(err && err.statusCode == 404) {
				write_json(name, value, /* revision*/ null, cb);
			}else{
				cb( NanoError(409) );
			}
	});
}

const read_json = function(name, cb) {
	let self = this;
	 self.document_exists(name, function(err, headers) {
		if(err && err.statusCode == 404) {
			cb( NanoError('ENOENT') );
		}else{
			let verdaccio = nano.db.use('verdaccio');
			verdaccio.get(name, function(err, body) {
				if (err) {
					cb( NanoError('ENOENT') );
				}else{
					cb(err, body.package);
				}
			});
		}
	});
};

module.exports.update_json = function(name, value, cb) {
	read_json(name, function(err, body) {
	if (headers.statusCode != 404) {
		write_json(name, value, body['_rev'], cb);
	}else{
		cb( NanoError(404) );
	}
	});
};

module.exports.delete_json = delete_json;
function delete_json(data, cb) {
	let verdaccio = nano.db.use('verdaccio');
	verdaccio.destroy(data.name, data['_rev'], function(err, body, header) {
		if (!err) {
			console.log('[local-couchdb.delete_json] ', [body]);
			return cb();
		}else{
			return cb();
		}
	});
}


module.exports.write_json = write_json;
function write_json(name, value, revision, cb) {
	let verdaccio = nano.db.use('verdaccio');
	// AQUI GUARDA E LFORMATO EN LA BASE DE DATOS
	let payload = {
		'_id': value.name, 
		'timestamp': Date.now(),
		'package': value,
		'docType': 'package.json',
	};
	if (revision) {
		payload ['_rev'] = revision;		// update code flow
	}

	verdaccio.insert( payload, function(err, body, header) {
		if (!err) {
			return cb();
		}else{
			return cb();
		}
	});
}

module.exports.read_json = read_json;

module.exports.read_stream = function(name, stream, callback) {
	let verdaccio = nano.db.use('verdaccio');
	var stream = MyStreams.readTarballStream();
	let self = this;
	verdaccio.attachment.get(name, name, function(err, body) {
		if(err) {
			stream.emit('error', Error[404]('no such file available'));
		 }else{
			stream.emit('open');
			stream.emit('content-length', body?body.length:-1);
			stream.end(body);
		}
	});
	return stream;
};

function delete_attachement_doc(docname, callback) {
	let verdaccio = nano.db.use('verdaccio');
	verdaccio.get(docname, function(err, body) {
		if (!err) {
			 let rev = body._rev;
				 verdaccio.destroy(docname, rev, function(err, body) {
						if (err)
								console.error(err);
						callback();
			 });
		}		else {
			callback();
		}
	});
}

module.exports.write_stream = function(name) {
	let stream = MyStreams.uploadTarballStream();

	fs.exists(name, function(exists) {
			if (exists) return stream.emit('error', FSError('EEXISTS'));

				delete_attachement_doc(name, function() {
			let verdaccio = nano.db.use('verdaccio');
			stream.emit('open');
			stream.pipe(
					verdaccio.attachment.insert(name, name, null, 'application/x-compressed', {'docType': 'tarball'} )
			);
			stream.emit('success');
				});
	});
	return stream;
};

module.exports.delete_stream = function(name, callback) {
	let verdaccio = nano.db.use('verdaccio');
	verdaccio.get(name, name, function(err, body) {
		if(!err) {
		 	// console.log('attempting to delete body.rev', body.rev);
		 	// console.log('attempting to delete body', body);
			verdaccio.destroy(name,
					body._rev, function(err, body) {
			});
		}
		if(callback) {
			return callback();
		}
	});
};


module.exports.document_exists = document_exists;

function document_exists(name, cb) {
	let verdaccio = nano.db.use('verdaccio');
	verdaccio.head(name, function(err, _, headers) {
		if(cb) {
			cb(err, headers);
		}
	});
}

module.exports.list_packages = list_packages;
function list_packages(callback) {
		let verdaccio = nano.db.use('verdaccio');
		verdaccio.view('nodepackages', 'list_node_packages', function(err, body) {
		if (!err) {
			body.rows.forEach(function(doc) {
				console.log('local-couchdb.list_packages', [doc.key, doc.value]);
			});
		}else{
			console.error('list_packages error!!!', err);
		}
		callback(err, body.rows);
	});
}


module.exports.search_packages = function(q, callback) {
	let limit = 50;
	if (!q || !callback) {
		let err = new Error('Invalid query/callback');
		console.err(err);
		if (callback)
			callback(err);
	}
	query = {
			q: q,
			limit: limit,
			reduce: false,
			include_docs: true,
		};
	let verdaccio = nano.db.use('verdaccio');
	verdaccio.search('search', 'all', query, function(err, result) {
		if (err) callback(err);
		else {
			let rows = [];
			try {
				if (typeof result === 'string') {
					var result = JSON.parse(result);
				}
				rows = result.rows;
			}			catch (e) {
				console.log(new Error('Search result is not a JSON'));
			}
			callback(null, rows);
		}
	});
};


let createDBIfNecessary = function() {
		nano.db.create('verdaccio', function(err, body) {
		if (!err) {
			console.log('database verdaccio created!');
			 let verdaccio = nano.db.use('verdaccio');
			 // Index
			verdaccio.insert(
				{
					'_id': '_design/views',
					'views': {
						'list_node_packages': {
							'map': function(doc) {
								let docType = doc['docType'];
								if (docType == 'package.json') {
									emit(doc['package']['name'], doc.timestamp);
								}
							},
						},
					},
				}, function(error, response) {
			});


			// Lucene
			verdaccio.insert(
					{
						_id: '_design/search',
						indexes: {
							all: {
								analyzer: {
									'name': 'perfield',
									'default': 'standard',
									'fields': {
										'name': 'keyword',
									}},
								index: 'function(doc) { var val = []; function idx(o) { for (var p in o) { if (!Array.isArray(o[p])) { if (typeof o[p] !== "object") val.push(o[p]); else idx(o[p]); } } } if (doc.docType === "package.json") { idx(doc); index("default", val.join(" "), { "store": true }); if (doc["package"] && doc["package"].name) { index("name", doc["package"].name, { "store": true }); } } }',
							},
						},
					}, function(error, response) {
			});
		}
	});
};


module.exports.unlink = function(filename, callback) {
	// required API when we were using filesystem calls
	if(callback) {
		return callback();
	}
};

createDBIfNecessary();


