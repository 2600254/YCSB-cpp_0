//
//  basic_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"
#include "basic_db.h"
#include "db_wrapper.h"
#include "db_sync_async_adapter.h"
#include "db_native_async_adapter.h"
#ifdef BIND_F2
#include "faster/f2_async_adapter.h"
#endif

namespace ycsbc {


std::map<std::string, DBFactory::DBCreator> &DBFactory::Registry() {
  static std::map<std::string, DBCreator> registry;
  return registry;
}

bool DBFactory::RegisterDB(std::string db_name, DBCreator db_creator) {
  Registry()[db_name] = db_creator;
  return true;
}

DB *DBFactory::CreateDB(utils::Properties *props, Measurements *measurements) {
  std::string db_name = props->GetProperty("dbname", "basic");
  DB *db = nullptr;
  std::map<std::string, DBCreator> &registry = Registry();
  if (registry.find(db_name) != registry.end()) {
    DB *new_db = (*registry[db_name])();
    new_db->SetProps(props);
    db = new DBWrapper(new_db, measurements);
  }
  return db;
}
#ifdef USE_ASYNC_TEST
AsyncDBInterface *DBFactory::CreateAsyncDB(utils::Properties *props, 
                                           Measurements *measurements,
                                           ycsbc::CoreWorkload *wl,
                                           utils::CountDownLatch *latch) {
  std::string db_name = props->GetProperty("dbname", "basic");
  AsyncDBInterface *db = nullptr;
  std::map<std::string, DBCreator> &registry = Registry();
  if (registry.find(db_name) != registry.end()) {
    DB *new_db = (*registry[db_name])();
    new_db->SetProps(props);
    if (db_name == "f2") {
#ifdef BIND_F2
      db = new F2AsyncAdapter(new_db, measurements, props, wl, latch);
#else
      db = nullptr;
#endif
    } else if (new_db->IsAsyncDB()){
      db = new DBNativeASyncAdapter(new_db, measurements, props, wl, latch);
    } else {
      db = new DBSyncAsyncAdapter(new_db, measurements, props, wl, latch);
    }
  }
  return db;
}

#endif // USE_ASYNC_TEST

} // ycsbc
