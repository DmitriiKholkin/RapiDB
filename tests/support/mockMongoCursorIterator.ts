export async function* mockMongoCursorIterator<T>(this: {
  toArray(): Promise<T[]>;
}): AsyncGenerator<T> {
  yield* await this.toArray();
}
