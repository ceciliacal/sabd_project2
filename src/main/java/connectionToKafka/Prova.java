package connectionToKafka;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Prova {

    private static final String datasetPath = "data/prj2_dataset.csv";
    private static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"), new SimpleDateFormat("dd-MM-yy HH:mm")};


    public static String checkDate(String dateTarget){
        String res = "";
        try {
            String string1 = "00:00";
            String string2 = "11:59";
            String string3 = "12:00";
            String string4 = "23:59";

            Date time1 = new SimpleDateFormat("HH:mm").parse(string1);
            Calendar calendar1 = Calendar.getInstance();
            calendar1.setTime(time1);
            calendar1.add(Calendar.DATE, 1);


            Date time2 = new SimpleDateFormat("HH:mm").parse(string2);
            Calendar calendar2 = Calendar.getInstance();
            calendar2.setTime(time2);
            calendar2.add(Calendar.DATE, 1);

            /*
            Date time3 = new SimpleDateFormat("HH:mm").parse(string3);
            Calendar calendar3 = Calendar.getInstance();
            calendar3.setTime(time3);
            calendar3.add(Calendar.DATE, 1);


            Date time4 = new SimpleDateFormat("HH:mm").parse(string4);
            Calendar calendar4 = Calendar.getInstance();
            calendar4.setTime(time4);
            calendar4.add(Calendar.DATE, 1);

             */

            String someRandomTime = "01:00:00";
            Date d = new SimpleDateFormat("HH:mm").parse(dateTarget);
            Calendar calendarTarget = Calendar.getInstance();
            calendarTarget.setTime(d);
            calendarTarget.add(Calendar.DATE, 1);

            Date x = calendarTarget.getTime();
            if (x.after(calendar1.getTime()) && x.before(calendar2.getTime())) {
                //checkes whether the current time is between 14:49:00 and 20:11:13.
                System.out.println(true);

                res = "am";
            }
            else{
                res = "pm";
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return res;
    }
    /*

    AM k: C20 v: [0x7086e9bed7ea1ca1b4ec7ea3955ef732792d29b8, 0xc35c9ebbf48cbb5857a868ce441824d0b2ff783a]
AM k: D20 v: [0x7086e9bed7ea1ca1b4ec7ea3955ef732792d29b8, 0xc35c9ebbf48cbb5857a868ce441824d0b2ff783a]
AM k: G33 v: [0xf04547eda556c5f289b24488ad61fed6f8258f6f]
AM k: G34 v: [0xf04547eda556c5f289b24488ad61fed6f8258f6f]
AM k: D1 v: [0x6d6794f3186f584637721a1e1789fd2e71c28195]
AM k: D19 v: [0x7086e9bed7ea1ca1b4ec7ea3955ef732792d29b8]
PM k: C20 v: [0x7086e9bed7ea1ca1b4ec7ea3955ef732792d29b8, 0xc35c9ebbf48cbb5857a868ce441824d0b2ff783a]
PM k: G33 v: [0xf04547eda556c5f289b24488ad61fed6f8258f6f]
PM k: D1 v: [0x6d6794f3186f584637721a1e1789fd2e71c28195]
PM k: D19 v: [0x7086e9bed7ea1ca1b4ec7ea3955ef732792d29b8]
     */

    public static void calculatePmRank(){

        Map<String, List<String>> pm = new HashMap<>();
        List<String> c20 = new ArrayList<>();
        c20.add("0x7086e9bed7ea1ca1b4ec7ea3955ef732792d29b8");
        c20.add("0xc35c9ebbf48cbb5857a868ce441824d0b2ff783a");

        List<String> d20 = new ArrayList<>();
        d20.add("0x7086e9bed7ea1ca1b4ec7ea3955ef732792d29b8");
        d20.add("0xc35c9ebbf48cbb5857a868ce441824d0b2ff783a");

        List<String> g33 = new ArrayList<>();
        g33.add("0xf04547eda556c5f289b24488ad61fed6f8258f6f");

        List<String> g34 = new ArrayList<>();
        g34.add("0xf04547eda556c5f289b24488ad61fed6f8258f6f");

        List<String> d1 = new ArrayList<>();
        d1.add("0x6d6794f3186f584637721a1e1789fd2e71c28195");

        List<String> d19 = new ArrayList<>();
        d19.add("0x7086e9bed7ea1ca1b4ec7ea3955ef732792d29b8");

        pm.put("C20", c20);
        pm.put("D20", d20);
        pm.put("G33", g33);
        pm.put("G34", g34);
        pm.put("D1", d1);
        pm.put("D19", d19);

        pm.forEach((k, v) -> System.out.println("PM k: " + k + " v: " + v));
        System.out.println("pm: "+pm);
        Map<Integer, List<String>> swappedKeyValue = new HashMap<>();
        Map<List<String>, Integer> result = new HashMap<>();

        int count = 0;
        int max = 2;

        //io ho idCella, listaIDs -> mi servono le 3 liste con size maggiore

        for (Map.Entry<String, List<String>> entry : pm.entrySet()) {
            List<String> values = new ArrayList<>();
            System.out.println("--------");
            System.out.println("entry: "+entry);
            int size = entry.getValue().size();
            System.out.println("k: size: "+size);
            System.out.println("v: entry.getKey(): "+entry.getKey());
            //se non c'è già quella chiave (cioè quel numero di navi con associato le celle)
            if (!swappedKeyValue.containsKey(size)){
                values.add(entry.getKey());
                swappedKeyValue.put(size, values);
            }
            //se c'è già la chiave che è la size del num di navi, devo appendere la cella ai values già presenti
            else{
                List<String> valuesGiaPresenti = entry.getValue();
                System.out.println("valuesGiaPresenti: "+valuesGiaPresenti);

                valuesGiaPresenti.add(entry.getKey());
                swappedKeyValue.put(size, valuesGiaPresenti);

            }

        }

        System.out.println("swappedKeyValue: "+swappedKeyValue);

        //key: num navi + alto, value: cella
        Map<Integer,List<String>> sortedCells = new TreeMap<>(Collections.reverseOrder());
        sortedCells.putAll(swappedKeyValue);

        System.out.println("OUTPUTQUERY2: sortedCells COMPLETA di PM: "+sortedCells);

        //prendo primi tre elementi da sortedCells
        for (Map.Entry<Integer, List<String>> entry : sortedCells.entrySet()){
            if (count>max) {
                break;
            }
            else{
                result.put(entry.getValue(), entry.getKey());
                count++;
            }

        }

        System.out.println("result: "+result);

    }

    public static void main(String[] args) throws IOException {

        calculatePmRank();

        /*
            List<String> ciao;
            //System.out.println("ciao size: "+ciao.size());

            List<Date> dateList = new ArrayList<>();
            String reader = "";
            String[] input = null;

            BufferedReader br = new BufferedReader(new FileReader(datasetPath));
            String header = br.readLine();
            while((reader = br.readLine()) != null) {
                input = reader.split(",");


                String dateStr = input[7];
                //System.out.println("---dateStr: "+dateStr);
                Date date = null;
                for (SimpleDateFormat dateFormat: dateFormats) {
                    try {
                        date = dateFormat.parse(dateStr);

                        //System.out.println("---cacca---  "+date);

                        break;
                    } catch (ParseException ignored) { }
                }

                //daterow è la data (Date)
                dateList.add(date);
            }

            //System.out.println("dateList: "+dateList);

            Collections.sort(dateList, new Comparator<Date>() {
                @Override
                public int compare(Date d1, Date d2) {
                    return d1.compareTo(d2);
                }
            });

            for (Date row: dateList) {
                System.out.println(row);
            }


         */

        }

/*
    public static void main(String[] args)  {

        List<Double> latSectors = new ArrayList<>();
        List<Double> lonSectors = new ArrayList<>();

        double lowerLat = 32.0;
        double upperLat = 45.0;
        double lowerLon = -6.0;
        double upperLon = 37.0;

        latSectors.add(lowerLat);
        lonSectors.add(lowerLon);

        double latRange = (upperLat-lowerLat);
        double lonRange = (upperLon-lowerLon);

        latRange = latRange / 10;

        System.out.println("LAT:");
        System.out.println(latRange);

        double sumLat = lowerLat;

        for (int i=0;i<9;i++){
            sumLat = sumLat + latRange;
            latSectors.add(sumLat);
        }

        latSectors.add(upperLat);

        System.out.println(latSectors);
        System.out.println("size: "+latSectors.size());
        System.out.println("LON:");

        lonRange = lonRange / 40;
        System.out.println(lonRange);

        double sumLon = lowerLon;

        for (int i=0;i<39;i++){
            sumLon = sumLon + lonRange;
            lonSectors.add(sumLon);
        }

        lonSectors.add(upperLon);

        System.out.println(lonSectors);
        System.out.println("size: "+lonSectors.size());

        //latitudine

        System.out.println("\n\n");
        double lat = 32.0;
        int indexLat = retrieveIndex(latSectors, lat);
        System.out.println("indexLat LAT = "+indexLat);
        String latLetter = getCharFromNumber(indexLat);
        System.out.println("settore LAT = "+latLetter);

        //longitudine

        double lon = -6.0;
        int indexLon = retrieveIndex(lonSectors, lon);
        String indexLonStr = String.valueOf(indexLon);
        System.out.println("settore LON = "+indexLon);

        String cell = latLetter.concat(indexLonStr);
        System.out.println("-- CELLA= "+cell);


    }

 */

    public static int retrieveIndex(List<Double> list, double lon){

        int res =0;

        if (lon==list.get(0)){
            res = list.indexOf(list.get(0))+1;
            return res;

        }
        if (lon==list.get(list.size()-1)){
            double lastElem = list.get(list.size()-1);
            res = list.indexOf(lastElem);
            return res;

        }

        System.out.println("PRIMA FOR res= "+res);
        for (int i=0;i<list.size();i++){

            //System.out.println("NEL FOR res= "+res);


            //System.out.println("lonSectors.get(i)= "+lonSectors.get(i)+ " lonSectors.get(i+1)= "+lonSectors.get(i+1));
            if (lon > list.get(i) && lon < list.get(i+1)){

                res = i+1;
                //System.out.println("STO NELL IF i+1 "+res);
                break;
            }

        }

        //System.out.println("DOPO FOR res= "+res);
        return res;
    }

    public static String getCharFromNumber(int i){

        List<String> letterList = Arrays.asList(new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I","J"});
        String res = letterList.get(i-1);

        return res;



    }


    }
