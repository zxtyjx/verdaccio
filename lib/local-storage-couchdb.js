'use strict';

let assert = require('assert');
let async = require('async');
let Crypto = require('crypto');
let Error = require('http-errors');
let Stream = require('readable-stream');
let URL = require('url');
let Logger = require('./logger');
let Search = require('./search');
let MyStreams = require('./streams');
let Utils = require('./utils');
let info_file = 'package.json';
let couchdb_storage = require('./local-couchdb');

//
// Implements Storage interface
// (same for storage.js, local-storage.js, up-storage.js)
//
function Storage(config) {
  let self = Object.create(Storage.prototype);
  self.config = config;
  self.logger = Logger.logger.child({sub: 'fs'});
  return self;
}

// returns the minimal package file
function get_boilerplate(name) {
  return {
    // standard things
    'name': name,
    'versions': {},
    'dist-tags': {},

    // our own object
    '_distfiles': {},
    '_attachments': {},
    '_uplinks': {},
  };
}

Storage.prototype._internal_error = function(err, file, message) {
  this.logger.error( {err: err, file: file}
                   , message + ' @{file}: @{!err.message}' );
  return Error[500]();
};

Storage.prototype.add_package = function(name, info, callback) {
  let self = this;
  self.logger.info( {name: name}
                  , 'add package @{name}');

  let storage = this.storage(name);
  if (!storage) return callback( Error[404]('this package cannot be added') );

  storage.create_json(name, info, function(err) {
    if (err && err.code === 'EEXISTS') {
      return callback( Error[409]('this package is already present') );
    }

    let latest = info['dist-tags'].latest;
    if (latest && info.versions[latest]) {
      Search.add(info.versions[latest]);
    }
    callback();
  });
};

Storage.prototype.remove_package = function(name, callback) {
  let self = this;
  self.logger.info( {name: name}
                  , 'remove package @{name}');

  let storage = self.storage(name);
  if (!storage) return callback( Error[404]('no such package available') );

  storage.read_json(name, function(err, data) {
    if (err) {
      if (err.code === 'ENOENT') {
        return callback( Error[404]('no such package available') );
      } else {
        return callback(err);
      }
    }

    // delete the package.json
    storage.delete_json(data, function(err) {
      if(err) self.logger.info( {name: name}
                  , 'unable to delete package @{name} ');


      // delete the tarballs
      self._normalize_package(data);
      let files = Object.keys(data._attachments);
      self.logger.info( {files: files}
                  , 'remove package  files @{files}');


      files.forEach(function(file) {
        self.remove_tarball(name, file);
      });
    });
    Search.remove(name);
    self.config.localList.remove(name);
    return callback();
  });
};
// LEE DESDE LA BASE DE DATOS SI EXISTE EL PAQUETE (SEA LOCAL O REMOTO)
Storage.prototype._read_create_package = function(name, callback) {
  let self = this;
  self.logger.info( {name: name}, 'read create package @{name}');
  let storage = self.storage(name);
  if (!storage) {
    let data = get_boilerplate(name);
    self._normalize_package(data);
    return callback(null, data);
  }
  storage.read_json(name, function(err, data) {
    // TODO: race condition
    if (err) {
      if (err.code === 'ENOENT') {
        // if package doesn't exist, we create it here
        data = get_boilerplate(name);
      } else {
        return callback(self._internal_error(err, info_file, 'error reading'));
      }
    }
    self._normalize_package(data);
    callback(null, data);
  });
};

// synchronize remote package info with the local one
// TODO: readfile called twice
Storage.prototype.update_versions = function(name, newdata, callback) {
  let self = this;
  self.logger.info( {name: name}
                  , 'update versions @{name}');
  self._read_create_package(name, function(err, data) {
    if (err) return callback(err);

    let change = false;
    for (let ver in newdata.versions) {
      if (data.versions[ver] == null) {
        let verdata = newdata.versions[ver];

        // we don't keep readmes for package versions,
        // only one readme per package
        delete verdata.readme;

        change = true;
        data.versions[ver] = verdata;

        if (verdata.dist && verdata.dist.tarball) {
          let filename = URL.parse(verdata.dist.tarball).pathname.replace(/^.*\//, '');
          // we do NOT overwrite any existing records
          if (data._distfiles[filename] == null) {
            let hash = data._distfiles[filename] = {
              url: verdata.dist.tarball,
              sha: verdata.dist.shasum,
            };

            if (verdata._verdaccio_uplink) {
              // if we got this information from a known registry,
              // use the same protocol for the tarball
              //
              // see https://github.com/rlidwka/sinopia/issues/166
              let tarball_url = URL.parse(hash.url);
              let uplink_url = URL.parse(self.config.uplinks[verdata._verdaccio_uplink].url);
              if (uplink_url.host === tarball_url.host) {
                tarball_url.protocol = uplink_url.protocol;
                hash.registry = verdata._verdaccio_uplink;
                hash.url = URL.format(tarball_url);
              }
            }
          }
        }
      }
    }
    for (let tag in newdata['dist-tags']) {
      if (!Array.isArray(data['dist-tags'][tag]) || data['dist-tags'][tag].length != newdata['dist-tags'][tag].length) {
        // backward compat
        var need_change = true;
      } else {
        for (let i=0; i<data['dist-tags'][tag].length; i++) {
          if (data['dist-tags'][tag][i] != newdata['dist-tags'][tag][i]) {
            var need_change = true;
            break;
          }
        }
      }

      if (need_change) {
        change = true;
        data['dist-tags'][tag] = newdata['dist-tags'][tag];
      }
    }
    for (let up in newdata._uplinks) {
      var need_change = !Utils.is_object(data._uplinks[up])
                     || newdata._uplinks[up].etag !== data._uplinks[up].etag
                     || newdata._uplinks[up].fetched !== data._uplinks[up].fetched;

      if (need_change) {
        change = true;
        data._uplinks[up] = newdata._uplinks[up];
      }
    }
    if (newdata.readme !== data.readme) {
      data.readme = newdata.readme;
      change = true;
    }

    if (change) {
      self.logger.debug('updating package info');
      self._write_package(name, data, function(err) {
        callback(err, data);
      });
    } else {
      callback(null, data);
    }
  });
};

Storage.prototype.add_version = function(name, version, metadata, tag, callback) {
  let self = this;
  self.update_package(name, function updater(data, cb) {
    // keep only one readme per package
    data.readme = metadata.readme;
    delete metadata.readme;
    // Clean up inline attachements
    if (data._attachments) {
      for (let k in data._attachments) {
        if (data._attachments[k].data)
          delete data._attachments[k];
      }
    }


    // if (data.versions[version] != null) {
    //   console.log('local-storage-couchdb.add_version already present? version', [self.config.localList, name, version])
    //   return cb( Error[409]('this version already present') )
    // }

    // if uploaded tarball has a different shasum, it's very likely that we have some kind of error
    if (Utils.is_object(metadata.dist) && typeof(metadata.dist.tarball) === 'string') {
      let tarball = metadata.dist.tarball.replace(/.*\//, '');
      if (Utils.is_object(data._attachments[tarball])) {
        if (data._attachments[tarball].shasum != null && metadata.dist.shasum != null) {
          if (data._attachments[tarball].shasum != metadata.dist.shasum) {
            return cb( Error[400]('shasum error, '
                                + data._attachments[tarball].shasum
                                + ' != ' + metadata.dist.shasum) );
          }
        }

        data._attachments[tarball].version = version;
      }
    }

    data.versions[version] = metadata;
    Utils.tag_version(data, version, tag, self.config);
    self.config.localList.add(name);
    cb();
  }, callback);
};

Storage.prototype.merge_tags = function(name, tags, callback) {
  let self = this;

  self.update_package(name, function updater(data, cb) {
    for (let t in tags) {
      if (tags[t] === null) {
        delete data['dist-tags'][t];
        continue;
      }

      if (data.versions[tags[t]] == null) {
        return cb( Error[404]('this version doesn\'t exist') );
      }

      Utils.tag_version(data, tags[t], t, self.config);
    }
    cb();
  }, callback);
};

Storage.prototype.replace_tags = function(name, tags, callback) {
  let self = this;

  self.update_package(name, function updater(data, cb) {
    data['dist-tags'] = {};

    for (let t in tags) {
      if (tags[t] === null) {
        delete data['dist-tags'][t];
        continue;
      }

      if (data.versions[tags[t]] == null) {
        return cb( Error[404]('this version doesn\'t exist') );
      }

      Utils.tag_version(data, tags[t], t, self.config);
    }
    cb();
  }, callback);
};

// currently supports unpublishing only
Storage.prototype.change_package = function(name, metadata, revision, callback) {
  let self = this;

  if (!Utils.is_object(metadata.versions) || !Utils.is_object(metadata['dist-tags'])) {
    return callback( Error[422]('bad data') );
  }

  self.update_package(name, function updater(data, cb) {
    for (let ver in data.versions) {
      if (metadata.versions[ver] == null) {
        self.logger.info( {name: name, version: ver}
                        , 'unpublishing @{name}@@{version}');
        delete data.versions[ver];

        for (let file in data._attachments) {
          if (data._attachments[file].version === ver) {
            delete data._attachments[file].version;
          }
        }
      }
    }
    data['dist-tags'] = metadata['dist-tags'];
    cb();
  }, function(err) {
    if (err) return callback(err);
    callback();
  });
};

Storage.prototype.remove_tarball = function(name, filename, revision, callback) {
  let self = this;
  self.logger.info( {name: name, filename: filename}
                  , 'remove tarball @{name} @{filename}');

  self.update_package(name, function updater(data, cb) {
    if (data._attachments[filename]) {
      delete data._attachments[filename];
      cb();
    } else {
      cb(Error[404]('no such file available'));
    }
  }, function(err) {
    if (err && callback) return callback(err);
    let storage = self.storage(name);

    if (storage) storage.delete_stream(filename, callback);
  });
};

Storage.prototype.add_tarball = function(name, filename, fullname) {
  var self = this;
  self.logger.info( {name: name, filename: filename}
                  , 'add tarball @{name} @{filename}');


  assert(Utils.validate_name(filename));

  let stream = MyStreams.uploadTarballStream();
  let _transform = stream._transform;
  let length = 0;
  let shasum = Crypto.createHash('sha1');

  stream.abort = stream.done = function() {};

  stream._transform = function(data) {
	try {
		shasum.update(data);
	}	catch (e) {
		console.error(e);
		// TODO we hit this area from times to times
		// debugger;
		throw e;
	}
    length += data.length;
    _transform.apply(stream, arguments);
  };

  var self = this;
  if (name === info_file || name === '__proto__') {
    process.nextTick(function() {
      stream.emit('error', Error[403]('can\'t use this filename'));
    });
    return stream;
  }

  let storage = self.storage(name);
  if (!storage) {
    process.nextTick(function() {
      stream.emit('error', Error[404]('can\'t upload this package'));
    });
    return stream;
  }

  let wstream = storage.write_stream(filename);

  wstream.on('error', function(err) {
    if (err.code === 'EEXISTS') {
      stream.emit('error', Error[409]('this tarball is already present'));
    } else if (err.code === 'ENOENT') {
      // check if package exists to throw an appropriate message
      self.get_package(name, function(_err, res) {
        if (_err) {
          stream.emit('error', _err);
        } else {
          stream.emit('error', err);
        }
      });
    } else {
      stream.emit('error', err);
    }
  });

  wstream.on('open', function() {
    // re-emitting open because it's handled in storage.js
    stream.emit('open');
  });
  wstream.on('end', function() {
    self.update_package(name, function updater(data, cb) {
      data._attachments[filename] = {
        shasum: shasum.digest('hex'),
      };
      cb();
    }, function(err) {
      if (err) {
        stream.emit('error', err);
      } else {
        stream.emit('success');
      }
    });
  });
  stream.abort = function() {
    wstream.abort();
  };
  stream.done = function() {
    if (!length) {
      stream.emit('error', Error[422]('refusing to accept zero-length file'));
      wstream.abort();
    } else {
      wstream.done();
    }
  };
  stream.pipe(wstream);

  return stream;
};

Storage.prototype.get_tarball = function(name, filename, callback) {
  assert(Utils.validate_name(filename));
  let self = this;

  let stream = MyStreams.readTarballStream();
  stream.abort = function() {
    if (rstream) rstream.abort();
  };

  let storage = self.storage(name);
  if (!storage) {
    process.nextTick(function() {
      stream.emit('error', Error[404]('no such file available'));
    });
    return stream;
  }

  var rstream = storage.read_stream(filename);
  rstream.on('error', function(err) {
    if (err && err.code === 'ENOENT') {
      stream.emit('error', Error(404, 'no such file available'));
    } else {
      stream.emit('error', err);
    }
  });
  rstream.on('content-length', function(v) {
    stream.emit('content-length', v);
  });
  rstream.on('open', function() {
    // re-emitting open because it's handled in storage.js
    stream.emit('open');
    rstream.pipe(stream);
  });
  return stream;
};

Storage.prototype.get_package = function(name, options, callback) {
  var self = this;
  self.logger.info( {name: name}
                  , 'get package @{name}');


  if (typeof(options) === 'function') callback = options, options = {};

  var self = this;
  let storage = self.storage(name);
  if (!storage) return callback( Error[404]('no such package available') );

  storage.read_json(name, function(err, result) {
    if (err) {
      if (err.code === 'ENOENT') {
        return callback( Error[404]('no such package available') );
      } else {
        return callback(self._internal_error(err, info_file, 'error reading'));
      }
    }
    self._normalize_package(result);
    callback(err, result);
  });
};

// walks through each package and calls `on_package` on them
Storage.prototype._each_package = function(on_package, on_end) {
  let self = this;
  let storages = {};

  storages[self.config.storage] = true;

  if (self.config.packages) {
    Object.keys(self.packages || {}).map(function(pkg) {
      if (self.config.packages[pkg].storage) {
        storages[self.config.packages[pkg].storage] = true;
      }
    });
  }
  let storage = self.storage('actuallyDoesNotMatterHere');
  storage.list_packages(function(error, packages) {
    async.eachSeries(packages, function(currpackage, cb) {
      on_package({
         name: currpackage.id,
         mtime: currpackage.value,
         }, cb);
    }, on_end);
  });
};

//
// This function allows to update the package thread-safely
//
// Arguments:
// - name - package name
// - updateFn - function(package, cb) - update function
// - callback - callback that gets invoked after it's all updated
//
// Algorithm:
// 1. lock package.json for writing
// 2. read package.json
// 3. updateFn(pkg, cb), and wait for cb
// 4. write package.json.tmp
// 5. move package.json.tmp package.json
// 6. callback(err?)
//
Storage.prototype.update_package = function(name, updateFn, _callback) {
  var self = this;
  self.logger.info( {name: name}
                  , 'update package @{name}');


  var self = this;
  let storage = self.storage(name);
  if (!storage) return _callback( Error[404]('no such package available') );
  storage.read_json(name, function(err, json) {
      if (err) return _callback(err);
      self._normalize_package(json);
      updateFn(json, function(err) {
        if(!err) {
           storage.write_json(json.name, json, json['_rev'], _callback);
         }else{
            _callback();
         }
      });
  });
};

Storage.prototype.search = function(startkey, options) {
  let self = this;

  let stream = new Stream.PassThrough({objectMode: true});

  self._each_package(function on_package(item, cb) {
      let convertedTimeToMillis = item.mtime;

      // if (convertedTimeToMillis > Number(startkey)) {
        if(true) {
        self.get_package(item.name, options, function(err, data) {
          if (err) return cb(err);

          let packageJson = data;

          let versions = Utils.semver_sort(Object.keys(packageJson.versions));
          let latest = versions[versions.length - 1];

          if (packageJson.versions[latest]) {
            stream.push({
              'name': packageJson.versions[latest].name,
              'description': packageJson.versions[latest].description,
              'dist-tags': {latest: latest},
              'maintainers': packageJson.versions[latest].maintainers ||
                                 [packageJson.versions[latest]._npmUser].filter(Boolean),
              'author': packageJson.versions[latest].author,
              'repository': packageJson.versions[latest].repository,
              'readmeFilename': packageJson.versions[latest].readmeFilename || '',
              'homepage': packageJson.versions[latest].homepage,
              'keywords': packageJson.versions[latest].keywords,
              'bugs': packageJson.versions[latest].bugs,
              'license': packageJson.versions[latest].license,
              'time': {modified: item.mtime ? new Date(item.mtime).toISOString() : undefined},
              'versions': {},
            });
          }
          cb();
        });
      } 
			// else{
      //   cb();
      // }
  }, function on_end(err) {
    if (err) return stream.emit('error', err);
    stream.end();
  });
  return stream;
};

// lucene search
Storage.prototype.lsearch = function(query, callback) {
	couchdb_storage.search_packages(query, function(err, r) {
		callback(err, r);
	});
};

Storage.prototype._normalize_package = function(pkg) {
  ['versions', 'dist-tags', '_distfiles', '_attachments', '_uplinks'].forEach(function(key) {
    if (!Utils.is_object(pkg[key])) {
        pkg[key] = {};
      }
    });
    if (typeof(pkg._rev) !== 'string') {
      pkg._rev = '0-0000000000000000';
    }
    // normalize dist-tags
    Utils.normalize_dist_tags(pkg);
};

Storage.prototype._write_package = function(name, json, callback) {
  // calculate revision a la couchdb
  if (typeof(json._rev) !== 'string') json._rev = '0-0000000000000000';
  let rev = json._rev.split('-');
  json._rev = ((+rev[0] || 0) + 1) + '-' + Crypto.pseudoRandomBytes(8).toString('hex');

  let storage = this.storage(name);
  if (!storage) return callback();
  storage.write_json(name, json, /* revision*/ null, callback);
};

Storage.prototype.storage = function(pkg) {
  return couchdb_storage;
};


module.exports = Storage;

