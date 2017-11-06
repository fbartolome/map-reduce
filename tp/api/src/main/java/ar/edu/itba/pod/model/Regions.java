package ar.edu.itba.pod.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Regions {

  private static Map<String,String> regions = null;

  private Regions(){
  }

  private static void initiateMap(){
    regions = new HashMap<>();
    regions.put("Buenos Aires","Región Buenos Aires");
    regions.put("Ciudad Autónoma de Buenos Aires","Región Buenos Aires");
    regions.put("Santa Fe","Región Centro");
    regions.put("Entre Ríos","Región Centro");
    regions.put("Córdoba","Región Centro");
    regions.put("San Luis","Región del Nuevo Cuyo");
    regions.put("Mendoza","Región del Nuevo Cuyo");
    regions.put("San Juan","Región del Nuevo Cuyo");
    regions.put("La Rioja","Región del Nuevo Cuyo");
    regions.put("La Pampa","Región Patagónica");
    regions.put("Neuquén","Región Patagónica");
    regions.put("Río negro","Región Patagónica");
    regions.put("Chubut","Región Patagónica");
    regions.put("Santa Cruz","Región Patagónica");
    regions.put("Tierra del Fuego","Región Patagónica");
    regions.put("Corrientes","Región del Norte Grande Argentino");
    regions.put("Misiones","Región del Norte Grande Argentino");
    regions.put("Formosa","Región del Norte Grande Argentino");
    regions.put("Chaco","Región del Norte Grande Argentino");
    regions.put("Tucumán","Región del Norte Grande Argentino");
    regions.put("Jujuy","Región del Norte Grande Argentino");
    regions.put("Salta","Región del Norte Grande Argentino");
    regions.put("Santiago del Estero","Región del Norte Grande Argentino");
    regions.put("Catamarca","Región del Norte Grande Argentino");
  }

  public static final String getRegion(String province){
    if(regions == null){
      initiateMap();
    }
    String ret = regions.get(province);
    if(ret == null){
      throw new IllegalArgumentException(province + " is not a valid province");
    }
    return ret;
  }

  public static final Set<String> getProvinces(){
    if(regions == null){
      initiateMap();
    }
    return regions.keySet();
  }

  public static final Collection<String> getRegions(){
    if(regions == null){
      initiateMap();
    }
    return regions.values();
  }

}
