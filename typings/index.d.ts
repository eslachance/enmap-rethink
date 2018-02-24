declare module 'enmap-rethink' {

	import * as Rethink from 'rethinkdb';

	export class EnmapLevel<K, V> {
		public features: {
			multiProcess: true;
			complexTypes: true;
			keys: 'multiple';
		};
		public name: string;
		public dbName: string;
		public path: string;
		public port: number;
		public host: string;
		public connection: Rethink.Connection;
		public db: Rethink.Db;
		public table: Rethink.Table;
		private defer: Promise;

		public init(enmap: Map): Promise;
		public close(): void;
		public set(key: K, value: V): void;
		public setAsync(key: K, value: V): Promise<void>;
		public delete(key: K): void;
		public deleteAsync(key: K): Promise<void>;
		public bulkDelete(): Promise<Rethink.WriteResult>;
		private ready(): void;
		private validateName(): void;
	}

}
