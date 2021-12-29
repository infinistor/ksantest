package org.example.s3tests;

public class FormFile {
	public String Name;
	public String ContentType;
	public String Body;
	 
	public FormFile(String Name, String ContentType, String Body)
	{
		this.Name = Name;
		this.ContentType = ContentType;
		this.Body = Body;
	}
	
	public String getName() {return Name;}
	public String getContentType() {return ContentType;}
	public String getBody() {return Body;}
	
	public void setName(String Name) {this.Name= Name;}
	public void setContentType(String ContentType) {this.ContentType= ContentType;}
	public void setBody(String Body) {this.Body= Body;}
}