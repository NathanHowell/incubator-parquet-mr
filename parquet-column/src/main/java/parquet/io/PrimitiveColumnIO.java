/**
 * Copyright 2012 Twitter, Inc.
 * Copyright 2014 GoDaddy, Inc.
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
package parquet.io;

import parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class PrimitiveColumnIO extends ColumnIO<PrimitiveType> {
  public PrimitiveColumnIO(
      final PrimitiveType fileSchema,
      final String name,
      final LeafInfo leafInfo) {
    super(fileSchema, name, leafInfo);
  }

  @Override
  public List<PrimitiveColumnIO> getLeafColumnIO() {
    final ArrayList<PrimitiveColumnIO> arr = new ArrayList<PrimitiveColumnIO>(1);
    arr.add(this);
    return arr;
  }

  @Override
  PrimitiveColumnIO getLast() {
    return this;
  }

  @Override
  PrimitiveColumnIO getFirst() {
    return this;
  }

  public boolean isFirst(int r) {
    return getFirst(r) == this;
  }

  private PrimitiveColumnIO getFirst(int r) {
    ColumnIO parent = getParent(r);
    return parent.getFirst();
  }

  public boolean isLast(int r) {
    return getLast(r) == this;
  }

  private PrimitiveColumnIO getLast(int r) {
    ColumnIO parent = getParent(r);
    return parent.getLast();
  }

  public ColumnIO<?>[] getColumnPath() {
    final ArrayList<ColumnIO<?>> path = new ArrayList<ColumnIO<?>>();
    ColumnIO<?> ref = this;
    do {
      path.add(ref);
      ref = ref.getParent();
    } while (ref != null);

    Collections.reverse(path);
    return path.toArray(new ColumnIO[path.size()]);
  }
}
