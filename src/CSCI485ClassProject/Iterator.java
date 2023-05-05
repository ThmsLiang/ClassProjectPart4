package CSCI485ClassProject;

import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.utils.ComparisonUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class Iterator {
  protected Cursor cursor;
  public enum Mode {
    READ,
    READ_WRITE
  }

  private Mode mode;

  public Mode getMode() {
    return mode;
  };

  public void setMode(Mode mode) {
    this.mode = mode;
  };

  public abstract Record next();

  public abstract void commit();

  public abstract void abort();

  public Cursor getCursor() { return cursor; }
}

class JoinIterator extends Iterator{
  private final SelectIterator outerIterator;
  private final SelectIterator innerIterator;
  private final ComparisonOperator operator;
  private final String outerAttribute;
  private final String innerAttribute;
  private final Object coefficient;
  private final AttributeType type;

  public JoinIterator(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames){
    coefficient = predicate.getRightHandSideValue();
    type = predicate.getLeftHandSideAttrType();
    operator = predicate.getOperator();
    outerAttribute = predicate.getLeftHandSideAttrName();
    innerAttribute = predicate.getRightHandSideAttrName();
    this.outerIterator = (SelectIterator) outerIterator;
    this.innerIterator = (SelectIterator) innerIterator;
  }
  @Override
  public Record next() {
    Record outerRecord, innerRecord;
    while (true) {
      outerRecord = outerIterator.next();

      // check null
      if (outerRecord == null) {
        return null;
      }

      Object outerVal = outerRecord.getValueForGivenAttrName(outerAttribute);
      SelectIterator innerIterator = new SelectIterator(this.innerIterator.getTableNameOfRecord(), this.innerIterator.getPredicate(), this.innerIterator.getmode(), this.innerIterator.getIsUsingIndex());

      // while loop to go over
      while(true){
        innerRecord = innerIterator.next();
        if(innerRecord == null){
          break;
        }
        Object innerVal = innerRecord.getValueForGivenAttrName(innerAttribute);
        if (type == AttributeType.INT && ComparisonUtils.compareTwoINT(outerVal, innerVal, coefficient, operator)) {
          Set<String> outerSet = new HashSet<>();
          HashMap<String,Record.Value> outerMap= outerRecord.getMapAttrNameToValue();
          for (Map.Entry<String, Record.Value> entry : outerMap.entrySet()) {
            outerSet.add(entry.getKey());
          }
          Set<String> innerSet = new HashSet<>();
          HashMap<String,Record.Value> innerMap= innerRecord.getMapAttrNameToValue();
          for (Map.Entry<String, Record.Value> entry : innerMap.entrySet()) {
            innerSet.add(entry.getKey());
          }

          Set<String> duplicates = new HashSet<>(outerSet);
          duplicates.retainAll(innerSet);
          Record record = new Record();
          for (Map.Entry<String, Record.Value> entry : outerMap.entrySet()) {
            String attrName = "";
            if(duplicates.contains(entry.getKey())){
              attrName += outerIterator.getTableNameOfRecord()+".";
            }
            attrName += entry.getKey();
            record.setAttrNameAndValue(attrName,entry.getValue().getValue());
          }
          for (Map.Entry<String, Record.Value> entry : innerMap.entrySet()) {
            String attrName = "";
            if(duplicates.contains(entry.getKey())){
              attrName += innerIterator.getTableNameOfRecord()+".";
            }
            attrName += entry.getKey();
            record.setAttrNameAndValue(attrName,entry.getValue().getValue());
          }
          return record;
        }

      }
    }
  }

  @Override
  public void commit() {
    outerIterator.commit();
    innerIterator.commit();
  }

  @Override
  public void abort() {
    outerIterator.abort();
    innerIterator.abort();
  }
}

class ProjectIterator extends Iterator{
  private String goalTableName = "";
  private RecordsImpl records;;

  private final String attrName;
  private final boolean hasNoDuplicates;
  private final SelectIterator inputIterator;
  private final SelectIterator goalIterator;
  private final ComparisonPredicate nonePredicate;
  private final Mode mode = Mode.READ;
  private final boolean isUsingIndex = false;
  public ProjectIterator(String tableName, String attrName, boolean duplicated){
    this.nonePredicate = new ComparisonPredicate();
    this.inputIterator = new SelectIterator(tableName,nonePredicate,mode,isUsingIndex);
    this.attrName = attrName;
    this.hasNoDuplicates = duplicated;

    if(duplicated){
      String[] attributeNames = new String[1];
      attributeNames[0] = this.attrName;
      AttributeType[] attributeType = new AttributeType[1];
      attributeType[0] = AttributeType.INT;
      long timestamp = System.currentTimeMillis();
      this.goalTableName = timestamp+ tableName;
      TableManagerImpl tableManager = new TableManagerImpl();
      tableManager.createTable(this.goalTableName, attributeNames, attributeType, attributeNames);


      records = new RecordsImpl();
      while (true) {
        Record record = inputIterator.next();
        if (record == null) {
          break;
        }
        Object[] val = new Object[1];
        val[0] = record.getValueForGivenAttrName(this.attrName);
        records.insertRecord(goalTableName, attributeNames, val, attributeNames, val);
      }
      goalIterator = new SelectIterator(goalTableName,nonePredicate,mode,isUsingIndex);
    }
    else{
      goalIterator = this.inputIterator;
    }
  }
  ProjectIterator(Iterator inputIterator, String attrName, boolean duplicate){
    this.nonePredicate = new ComparisonPredicate();
    this.inputIterator = (SelectIterator) inputIterator;
    this.attrName = attrName;
    this.hasNoDuplicates = duplicate;

    if(duplicate){
      String[] attributeNames = new String[1];
      attributeNames[0] = this.attrName;
      AttributeType[] attributeType = new AttributeType[1];
      attributeType[0] = AttributeType.VARCHAR;
      long timestamp = System.currentTimeMillis();
      this.goalTableName = timestamp+this.inputIterator.getTableNameOfRecord();
      TableManagerImpl tableManager = new TableManagerImpl();
      tableManager.createTable(this.goalTableName, attributeNames, attributeType, attributeNames);

      records = new RecordsImpl();
      while (true) {
        Record record = inputIterator.next();
        if (record == null) {
          break;
        }
        Object[] val = new Object[1];
        val[0] = record.getValueForGivenAttrName(attrName);
        records.insertRecord(goalTableName, attributeNames, val, attributeNames, val);
      }
      goalIterator = new SelectIterator(goalTableName,nonePredicate,mode,isUsingIndex);
    }
    else{
      goalIterator =  this.inputIterator;
    }
  }
  @Override
  public Record next() {
    if(hasNoDuplicates){
      return goalIterator.next();
    }
    else{
      Record originalRecord = goalIterator.next();
      if(originalRecord==null) return null;
      Record projectRecord = new Record();
      projectRecord.setAttrNameAndValue(attrName, originalRecord.getValueForGivenAttrName(attrName));
      return projectRecord;
    }
  }

  @Override
  public void commit() {
    inputIterator.commit();
    goalIterator.commit();
  }

  @Override
  public void abort() {
    inputIterator.abort();
    goalIterator.abort();
  }
}

class SelectIterator extends Iterator{
  private boolean isFirst;
  private final RecordsImpl records;
  private final String tableName;
  private final ComparisonPredicate predicate;
  private final Iterator.Mode mode;
  private final boolean isUsingIndex;
  SelectIterator(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex){
    this.tableName = tableName;
    this.predicate = predicate;
    this.mode = mode;
    this.isUsingIndex = isUsingIndex;
    records = new RecordsImpl();
    isFirst = true;
    Cursor.Mode cursor_mode = null;

    if(mode==Iterator.Mode.READ){
      cursor_mode = Cursor.Mode.READ;
    }

    else if(mode== Iterator.Mode.READ_WRITE){
      cursor_mode = Cursor.Mode.READ_WRITE;
    }

    if(predicate.getPredicateType()==ComparisonPredicate.Type.NONE){
      cursor = records.openCursor(tableName,cursor_mode);
    }
    else{
      cursor = records.openCursor(tableName,predicate,cursor_mode,isUsingIndex);
    }
  }
  public Record first(){
    return records.getFirst(cursor);
  }
  @Override
  public Record next() {
    if(isFirst){
      isFirst = false;
      return first();
    }
    else {
      return records.getNext(cursor);
    }
  }

  @Override
  public void commit() {
    records.commitCursor(cursor);
  }

  @Override
  public void abort() {
    records.abortCursor(cursor);
  }

  public String getTableNameOfRecord(){
    return tableName;
  }

  public ComparisonPredicate getPredicate(){
    return predicate;
  }

  public Iterator.Mode getmode(){
    return mode;
  }

  public boolean getIsUsingIndex(){
    return isUsingIndex;
  }

}



