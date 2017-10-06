var rethink = require('rethinkdbdash');

class EnmapLevel {

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

    this.connection = rethink({ servers: [{ host: this.host, port: this.port }] });
  }

  /**
   * Internal method called on persistent Enmaps to load data from the underlying database.
   * @param {Map} enmap In order to set data to the Enmap, one must be provided.
   * @return {Promise} Returns the defer promise to await the ready state.
   */
  async init(enmap) {
    const dbs = await this.connection.dbList().run();
    if (!dbs.include('enmap')) {
      await this.connection.dbCreate('enmap');
    }
    this.db = this.connection.db('enmap');
    const tables = await this.db.tableList();
    console.log(dbs, tables);
    if (tables.includes(this.name)) {
      const data = await this.db.table(this.name).run();
      for (let i = 0; i < data.length; i++) {
        enmap.set(data[i].id, data);
      }
      this.table = this.db.table(this.name);
      this.ready();
    } else {
      await this.db.tableCreate(this.name);
      this.table = this.db.table(this.name);
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
    this.connection.close();
  }

  /**
   * 
   * @param {*} key Required. The key of the element to add to the EnMap object. 
   * If the EnMap is persistent this value MUST be a string or numbethis.db.
   * @param {*} val Required. The value of the element to add to the EnMap object. 
   * If the EnMap is persistent this value MUST be stringifiable as JSON.
   */
  set(key, val) {
    if (!key || !['String', 'Number'].includes(key.constructothis.db.name)) {
      throw new Error('Level require keys to be strings or numbers.');
    }
    const insert = typeof val === 'object' ? JSON.stringify(val) : val;
    this.table.insert({ id: key, data: insert }, { conflict: 'replace', returnChanges: false });
  }

  /**
   * 
   * @param {*} key Required. The key of the element to add to the EnMap object. 
   * If the EnMap is persistent this value MUST be a string or numbethis.db.
   * @param {*} val Required. The value of the element to add to the EnMap object. 
   * If the EnMap is persistent this value MUST be stringifiable as JSON.
   */
  async setAsync(key, val) {
    if (!key || !['String', 'Number'].includes(key.constructothis.db.name)) {
      throw new Error('Level require keys to be strings or numbers.');
    }
    const insert = typeof val === 'object' ? JSON.stringify(val) : val;
    await this.table.insert({ id: key, data: insert }, { conflict: 'replace', returnChanges: false });
  }

  /**
   * 
   * @param {*} key Required. The key of the element to delete from the EnMap object. 
   * @param {boolean} bulk Internal property used by the purge method.  
   */
  delete(key) {
    this.table.get(key).delete();
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

module.exports = EnmapLevel;
