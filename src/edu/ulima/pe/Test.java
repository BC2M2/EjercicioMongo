package edu.ulima.pe;

import com.mongodb.*;
import java.net.UnknownHostException;
import java.util.*;

public class Test {
    
    MongoClient cliente = null;
    DB database = null;
    DBCollection coleccion = null;

    public static void main(String[] args) {
        Test t = new Test();
        t.cargarMongo();
        t.insertar();
        
    }
    
    private void cargarMongo(){
        try {
            cliente = new MongoClient();
            database = cliente.getDB("1542");
            coleccion = database.getCollection("alumnos");
        } catch (UnknownHostException ex) {
            System.out.println("Error en coneccion");
        }
        
    }
    
    private void insertar(){
        BasicDBObject doc1 = new BasicDBObject();
        doc1.put("codigo", 20120396);
        doc1.put("nombre", "Bryan");
        doc1.put("apellido", "Cuya");
        doc1.put("nivel", 1);
        
        BasicDBObject doc2 = new BasicDBObject();
        doc2.put("telefono", 941057553);
        doc2.put("correo", "b@gmail.com");
        doc2.put("genero", "M");
        
        doc1.put("Otros Datos", doc2);
        
        coleccion.insert(doc1);
        
        BasicDBObjectBuilder doc3 = new BasicDBObjectBuilder().start()
                .add("codigo", 20122030)
                .add("nombre", "Evelin")
                .add("apellido","Ortega")
                .add("nivel", 2);
        BasicDBObjectBuilder doc4 = new BasicDBObjectBuilder().start()
                .add("correo", "e@gmail.com")
                .add("genero", "F");
                
        doc3.add("Otros Datos", doc4.get());
        
        coleccion.insert(doc3.get());
        
        Map<String,Object> doc5 = new HashMap<>();
        doc5.put("codigo", 20120358);
        doc5.put("nombre", "Gary");
        doc5.put("apellido", "Cordova");
        doc5.put("nivel", 1);
        Map<String,Object> doc6 = new HashMap<>();
        doc6.put("correo", "g@gmail.com");
        doc6.put("genero", "M");
        
        doc5.put("Otros Datos", doc6);
        
        BasicDBObject basico = new BasicDBObject(doc5);
        
        coleccion.insert(basico);
        
        BasicDBObject a4 = new BasicDBObject();
        a4.put("codigo", 20121317);
        a4.put("nombre", "Estefany");
        a4.put("apellido", "Valdivieso");
        a4.put("nivel", 3);
        
        BasicDBObject a41 = new BasicDBObject();
        a41.put("correo", "ev@gmail.com");
        a41.put("genero", "F");
        
        a4.put("Otros Datos", a41);
        
        coleccion.insert(a4);
        
        BasicDBObjectBuilder a5 = new BasicDBObjectBuilder().start()
                .add("codigo", 20121846)
                .add("nombre", "Diana")
                .add("apellido","Rupaylla")
                .add("nivel", 2);
        BasicDBObjectBuilder a51 = new BasicDBObjectBuilder().start()
                .add("correo", "d@gmail.com")
                .add("genero", "F");
                
        a5.add("Otros Datos", a51.get());
        
        coleccion.insert(a5.get());
        
        Map<String,Object> a6 = new HashMap<>();
        a6.put("nombre", "George");
        a6.put("apellido", "Ortega");
        a6.put("nivel", 2);
        Map<String,Object> a61 = new HashMap<>();
        a61.put("genero", "M");
        
        a6.put("Otros Datos", a61);
        
        BasicDBObject a62 = new BasicDBObject(a6);
        
        coleccion.insert(a62);
        
        
    }
    
    private void eliminar() {
        BasicDBObject doc = new BasicDBObject();
        //Elimina todo los que cumplan
        //En este caso todos los que tengan codigo = 20120396
        doc.put("codigo", 20120396);
        coleccion.remove(doc);
    }
    
    //db.<<NOMBRE DE COLECCION>>.find()
    
    private void selectTodosLosDocumentos()  {
        DBCursor  c1 = coleccion.find();
        imprimirCursor(c1);
    }

    
    //db.<<NOMBRE DE COLECCION>>.findOne()
    
    private void selectPrimerDocumento() {
        DBObject o = coleccion.findOne();
         System.out.println( o );
    }

    //db.<<NOMBRE DE COLECCION>>.find( { 'genero' : 'F' } )
    
    private void selectCOnCriterioSimple() {
        BasicDBObject doc1 = new BasicDBObject()
                                              .append("genero", "F");
        
        DBCursor c2 = coleccion.find(doc1);
        imprimirCursor(c2);
    }
    
    //db.<<NOMBRE DE COLECCION>>.find( { 'genero' : 'F' }, { 'nivel':1,'nombre':1} )
    
    private void selectConCriterioSimpleYCampos() {
            BasicDBObject whereQuery = new BasicDBObject();
            whereQuery.put("genero", "M");
            
            BasicDBObject campos = new BasicDBObject();
            campos.put("nombre", 1);
            campos.put("nivel",1);
            
            DBCursor c3 = coleccion.find(whereQuery, campos);
            
            imprimirCursor(c3);
    }
    
    /*
    db.<<NOMBRE DE COLECCION>>.find( {
       'genero' : 'F' , 
       $and : [ { 'nivel':'2' } , {'apellido': 'Ortega' } ] 
      })
    */
    private void selectMultiplesCriterios() {
            BasicDBObject query = new BasicDBObject();
            
            List<BasicDBObject> condiciones = new ArrayList<>();
            condiciones.add( new BasicDBObject("genero","F") );
            condiciones.add( new BasicDBObject("nivel",2) );
            condiciones.add( new BasicDBObject("apellido","Ortega") );
            
            query.put("$and",condiciones);
            System.out.println( query.toString() );

            DBCursor c4 = coleccion.find(query);
            imprimirCursor(c4);
    }
        
    /*
    db.cartoon.find( {
       $or : [ { 'apellido': 'Ortega' } , {'apellido': 'Cordova' } ] 
      })
    */
    private void selectMultiplesCriteriosconOR() {
            BasicDBObject query = new BasicDBObject();
            
            List<BasicDBObject> condiciones = new ArrayList<>();
            condiciones.add( new BasicDBObject("apellido","Cordova") );
            condiciones.add( new BasicDBObject("apellido","Ortega") );
            
            query.put("$or",condiciones);
            System.out.println( query.toString() );

            DBCursor c5 = coleccion.find(query);
            imprimirCursor(c5);
    }
    
    private void imprimirCursor(DBCursor cursor){
        while(cursor.hasNext()){
            System.out.println(cursor.next());
        }
    }
    
}
