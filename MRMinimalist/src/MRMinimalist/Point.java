
package MRMinimalist;
/**
 *
 * @author Hussein A Hassan
 */
public class Point {
    
    double x;
    double y;
    double z;
    int prev;
    int next;
   
  public Point(double X, double Y, double Z, int Prev, int Next){
  
  x=X;
  y=Y;
  z=Z;
  prev=Prev;
  next=Next;
    
  }
  
  public Point(){
  //Id=0;
  x=0;
  y=0;
  z=0;
  prev=0;
  next=0;
    
  }
   
     
}