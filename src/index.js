var rethink = require('rethinkdbdash');

class EnmapProvider {

  constructor(options) {
    this.defer = new Promise((resolve) => {
      this.ready = resolve;
    });

    if (!options.name) throw new Error('Must provide options.name');
    this.name = options.name;
    this.dbName = options.dbName || 'enmap';
    this.validateName();

    this.port = options.port || 28015;
    this.host = options.host || 'localhost';
  }

  /**
   * Internal method called on persistent Enmaps to load data from the underlying database.
   * @param {Map} enmap In order to set data to the Enmap, one must be provided.
   * @return {Promise} Returns the defer promise to await the ready state.
   */
  async init(enmap) {
    this.enmap = enmap;
    this.connection = await rethink({ servers: [{ host: this.host, port: this.port }], silent: true });
    const dbs = await this.connection.dbList().run();
    if (!dbs.includes(this.dbName)) {
      await this.connection.dbCreate(this.dbName);
    }
    this.connection = this.connection.db(this.dbName);
    const tables = await this.connection.tableList();
    if (!tables.includes(this.name)) {
      await this.connection.tableCreate(this.name);
    }
    this.db = this.connection.table(this.name);
    if (this.fetchAll) {
      await this.fetchEverything();
      this.ready();
    } else {
      this.ready();
    }
    return this.defer;
  }

  /**
   * Shuts down the underlying persistent enmap database.
   * @return {Promise<*>} The promise of the database closing operation.
   */
  close() {
    return this.connection.getPoolMaster().drain();
  }

  /**
   * Fetches a specific key or array of keys from the database, adds it to the EnMap object, and returns its value.
   * @param {(string|number)} key The key to retrieve from the database.
   * @return {Promise<*>} The value obtained from the database. 
   */
  async fetch(key) {
    const row = await this.db.get(key).run();
    if (row) this.enmap.set(key, row.value);
    return Promise.resolve(row.data);
  }

  /** 
   * Fetches all non-cached values from the database, and adds them to the enmap.
   * @return {Promise<Map>} The promise of a cached Enmap.
  */
  async fetchEverything() {
    const data = await this.db.run();
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      let parsedValue = row.data;
      if (row.value[0] === '[' || row.value[0] === '{') {
        parsedValue = JSON.parse(row.value);
      }
      this.enmap.set(row.id, parsedValue, false);
    }
    return this;
  }
  /**
   * Set a value to the database.
   * @param {(string|number)} key Required. The key of the element to add to the EnMap object.
   * If the EnMap is persistent this value MUST be a string or number.
   * @param {*} val Required. The value of the element to add to the EnMap object.
   * If the EnMap is persistent this value MUST be stringifiable as JSON.
   * @return {Promise<*>} Promise returned by the database after insertion.
   */
  set(key, val) {
    if (!key || !['String', 'Number'].includes(key.constructor.name)) {
      throw new Error('Rethink requires keys to be strings or numbers.');
    }
    const insert = typeof val === 'object' ? JSON.stringify(val) : val;
    return this.db.insert({ id: key, data: insert }, { conflict: 'replace', returnChanges: false }).catch(console.error);
  }

  /**
   * 
   * @param {*} key Required. The key of the element to delete from the EnMap object. 
   * @param {boolean} bulk Internal property used by the purge method.
   * @return {Promise<*>} Promise returned by the database after deletion
   */
  delete(key) {
    return this.db.get(key).delete();
  }

  /**
   * Checks if a key is present in the database (used when not all rows are fetched).
   * @param {(string|number)} key Required. The key of the element we're checking in the database.
   * @return {Promise<boolean>} Whether the key exists in the database.
   */
  hasAsync(key) {
    return this.db.get(key);
  }

  /**
   * Delete all entries from the database
   * @returns {r.WriteResult}
   */
  bulkDelete() {
    return this.db.delete();
  }

  /**
   * Internal method used by Enmap to retrieve provider's correct version.
   * @return {string} Current version number.
   */
  getVersion() {
    return require('./package.json').version;
  }

  /**
   * Internal method used to validate persistent enmap names (valid Windows filenames);
   * @private
   */
  validateName() {
    this.name = this.name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
  }

}

module.exports = EnmapProvider;
