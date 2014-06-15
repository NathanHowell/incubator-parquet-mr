/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column;

/**
 * Container which can produce a ColumnReader for any given column in a schema.
 *
 * @author Julien Le Dem
 */
public interface ColumnReadStore {

  /**
   * @param physicalDescriptor the column to read
   * @param logicalDescriptor the column to read
   * @return the column reader for that descriptor
   */
  public ColumnReader getColumnReader(ColumnDescriptor physicalDescriptor, ColumnDescriptor logicalDescriptor);

  /**
   * @param columnDescriptor the column to read
   * @return the column reader for that descriptor
   */
  public ColumnReader getColumnReader(ColumnDescriptor columnDescriptor);
}
