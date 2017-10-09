var r = require('rethinkdbdash');

class EnmapRethink {

  constructor(options) {
    this.defer = new Promise((resolve) => {
      this.ready = resolve;
    });

    this.features = {
      multiProcess: true,
      complexTypes: true,
      keys: 'multiple'
    };

    if (!options.name) throw new Error('Must provide options.name');
    this.name = options.name;
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
    this.connection = await r({ servers: [{ host: this.host, port: this.port }] });
    const dbs = await this.connection.dbList().run();
    if (!dbs.includes('enmap')) {
      await this.connection.dbCreate('enmap');
    }
    this.db = this.connection.db('enmap');
    const tables = await this.db.tableList();
    if (tables.includes(this.name)) {
      const data = await this.db.table(this.name).run();
      this.table = this.db.table(this.name);
      for (let i = 0; i < data.length; i++) {
        enmap.set(data[i].id, data, false);
      }
      console.log(`Loaded ${data.length} rows from Rethink`);
      this.ready();
    } else {
      await this.db.tableCreate(this.name);
      this.table = this.db.table(this.name);
      console.log('Initialized new table in Rethink');
      this.ready();
    }
    return this.defer;
  }

  /**
   * Internal method used to validate persistent enmap names (valid Windows filenames);
   */
  validateName() {
    this.name = this.name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
  }

  /**
   * Shuts down the underlying persistent enmap database.
   */
  close() {
    // this.connection.getPoolMaster().drain();
  }

  /**
   * 
   * @param {*} key Required. The key of the element to add to the EnMap object. 
   * If the EnMap is persistent this value MUST be a string or number.
   * @param {*} val Required. The value of the element to add to the EnMap object. 
   * If the EnMap is persistent this value MUST be stringifiable as JSON.
   */
  set(key, val) {
    if (!key || !['String', 'Number'].includes(key.constructor.name)) {
      throw new Error('Rethink requires keys to be strings or numbers.');
    }
    const insert = typeof val === 'object' ? JSON.stringify(val) : val;
    this.table.insert({ id: key, data: insert }, { conflict: 'replace', returnChanges: false }).catch(console.error);
  }

  /**
   * 
   * @param {*} key Required. The key of the element to add to the EnMap object. 
   * If the EnMap is persistent this value MUST be a string or number.
   * @param {*} val Required. The value of the element to add to the EnMap object. 
   * If the EnMap is persistent this value MUST be stringifiable as JSON.
   */
  async setAsync(key, val) {
    if (!key || !['String', 'Number'].includes(key.constructor.name)) {
      throw new Error('Rethink requires keys to be strings or numbers.');
    }
    const insert = typeof val === 'object' ? JSON.stringify(val) : val;
    await this.table.insert({ id: key, data: insert }, { conflict: 'replace', returnChanges: false }).catch(console.error);
  }

  /**
   * 
   * @param {*} key Required. The key of the element to delete from the EnMap object. 
   * @param {boolean} bulk Internal property used by the purge method.  
   */
  delete(key) {
    this.table.get(key).delete();
  }

  bulkDelete() {
    return this.table.delete();
  }

  /**
   * 
   * @param {*} key Required. The key of the element to delete from the EnMap object. 
   * @param {boolean} bulk Internal property used by the purge method.  
   */
  async deleteAsync(key) {
    await this.table.get(key).delete();
  }

}

module.exports = EnmapRethink;
