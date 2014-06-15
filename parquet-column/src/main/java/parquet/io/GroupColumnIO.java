/**
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

import parquet.schema.GroupType;

import java.util.ArrayList;
import java.util.List;

public class GroupColumnIO<T extends GroupType> extends ColumnIO<T> {
  private final List<ColumnIO<?>> children;

  public GroupColumnIO(
      final T type,
      final String name,
      final LeafInfo leafInfo,
      final List<ColumnIO<?>> children) {
    super(type, name, leafInfo);
    this.children = children;
  }

  @Override
  void setParent(final ColumnIO<?> parent) {
    super.setParent(parent);
    for (final ColumnIO<?> child : children) {
      child.setParent(this);
    }
  }

  @Override
  PrimitiveColumnIO getLast() {
    return children.get(children.size() - 1).getLast();
  }

  @Override
  PrimitiveColumnIO getFirst() {
    return children.get(0).getFirst();
  }

  public List<ColumnIO<?>> getChildren() {
    return children;
  }

  @Override
  public final List<PrimitiveColumnIO> getLeafColumnIO() {
    final ArrayList<PrimitiveColumnIO> arr = new ArrayList<PrimitiveColumnIO>();
    for (final ColumnIO<?> child : children) {
      arr.addAll(child.getLeafColumnIO());
    }
    return arr;
  }
}
