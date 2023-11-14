package simpledb.parse;

public class DropIndexData {
    private String idxname, tblname, fldname;

    public DropIndexData(String idxname, String tblname, String fldname) {
        this.idxname = idxname;
        this.tblname = tblname;
        this.fldname = fldname;
    }

    public String getIdxname() {
        return idxname;
    }

    public String getTblname() {
        return tblname;
    }

    public String getFldname() {
        return fldname;
    }
}
