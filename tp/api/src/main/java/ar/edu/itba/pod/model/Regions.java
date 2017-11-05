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
    regions.put("buenos aires","Región Buenos Aires");
    regions.put("ciudad autónoma de buenos aires","Región Buenos Aires");
    regions.put("santa fe","Región Centro");
    regions.put("entre ríos","Región Centro");
    regions.put("córdoba","Región Centro");
    regions.put("san luis","Región del Nuevo Cuyo");
    regions.put("mendoza","Región del Nuevo Cuyo");
    regions.put("san juan","Región del Nuevo Cuyo");
    regions.put("la rioja","Región del Nuevo Cuyo");
    regions.put("la pampa","Región Patagónica");
    regions.put("neuquén","Región Patagónica");
    regions.put("río negro","Región Patagónica");
    regions.put("chubut","Región Patagónica");
    regions.put("santa cruz","Región Patagónica");
    regions.put("tierra del fuego","Región Patagónica");
    regions.put("corrientes","Región del Norte Grande Argentino");
    regions.put("misiones","Región del Norte Grande Argentino");
    regions.put("formosa","Región del Norte Grande Argentino");
    regions.put("chaco","Región del Norte Grande Argentino");
    regions.put("tucumán","Región del Norte Grande Argentino");
    regions.put("jujuy","Región del Norte Grande Argentino");
    regions.put("salta","Región del Norte Grande Argentino");
    regions.put("santiago del estero","Región del Norte Grande Argentino");
    regions.put("catamarca","Región del Norte Grande Argentino");
  }

  public static final String getRegion(String province){
    if(regions == null){
      initiateMap();
    }
    String ret = regions.get(province.toLowerCase());
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
