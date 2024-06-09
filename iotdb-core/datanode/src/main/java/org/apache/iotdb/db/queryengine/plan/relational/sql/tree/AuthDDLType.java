package org.apache.iotdb.db.queryengine.plan.relational.sql.tree;

public enum AuthDDLType {
  CREATE_USER,
  CREATE_ROLE,
  DROP_USER,
  DROP_ROLE,
  GRANT_USER_ROLE,
  REVOKE_USER_ROLE,
  GRANT_USER,
  GRANT_ROLE,
  REVOKE_USER,
  REVOKE_ROLE,
  LIST_USER,
  LIST_ROLE,
}
