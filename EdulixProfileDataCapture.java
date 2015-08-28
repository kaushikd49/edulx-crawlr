package com.app.practice;

import com.app.db.ProfileDataMapper;
import com.google.common.collect.Lists;
import com.libs.ip.Global;
import com.libs.platform.core.FoxDBLayer;
import com.libs.platform.fox.dbl.DatabaseEndpoint;
import com.libs.platform.fox.dbl.FoxDBLRegistry;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import static com.app.db.ProfileDataMapper.*;
import static org.apache.commons.collections.CollectionUtils.subtract;
/**
 * User: kaushik Date: 13/11/13 Time: 9:58 AM
 */
public class EdulixProfileDataCapture
{
  public static final Map<String,String> reMap = new HashMap<String, String>();

  static
  {
    String[] a = {"Major", "Score:", "University/College", "Topper's Grade", "AWA:", "Quantitative:", "Specialization", "Grade Scale", "Verbal:",
      "Grade", "Edulix Nickname", "Term and Year", "Program", "Department", "Real Name", "TWE (Essay):"};
    String[] b = {"major", "toefl", "university", "toppergrade", "awa", "quant", "specialization", "grade_scale",
                  "verbal", "grade", "nickname", "term", "program", "dept", "name", "essay"};

    for (int i = 0; i < a.length; i++)
      reMap.put(a[i], b[i]);
  }

  public static final String EDULIX_PROFILE_SEARCH_URL =
    "http://edulix.com/unisearch/user.php?uid=";
  public static final String EDULIX_UNI_COMMON_URL =
    "http://edulix.com/unisearch/univreview.php?univid=";
  public static final String COLLEGE_INDICATOR = "Universities Applied";
  public static final int MAX_THREADS = 10;
  public static final int PARTITION_SIZE = 100;

  private final ProfileDataMapper mapper;
  private final FoxDBLayer dbLayer;
  private Pattern discardPattern;

  public EdulixProfileDataCapture()
  {
    dbLayer = FoxDBLRegistry.getDBLayer(Global.DbEndpoint.FK_INVENTORY_DB, DatabaseEndpoint
      .EndpointType.READ_WRITE);
    mapper = dbLayer.getMapper(ProfileDataMapper.class);
    discardPattern = Pattern.compile("TOEFL|GRE|Applied");
  }

  public static void main(String[] args) throws IOException, InterruptedException
  {
    String confPath = "/Users/kaushik/work/git/fk-erp-inventory-planning/deb/etc/fk-erp-inventory-planning/config";
    String logPath = "/Users/kaushik/work/git/fk-erp-inventory-planning/deb/var/log";

    Global.init(EdulixProfileDataCapture.class, confPath, logPath);
    EdulixProfileDataCapture epd = new EdulixProfileDataCapture();

    List<Integer> univIdList = Arrays
      .asList(
       153, 138, 139, 969, 142, 144, 1510, 867, 1507, 1301, 1366, 880, 79, 153, 206, 1319,
       1586, 570, 647, 1410, 419, 867, 918, 408,1127, 1559, 1559,1565,373,1469,1471,1472,1470);

    Set<Integer> univIdSet  = new HashSet<Integer>(univIdList);

    epd.captureUnivsParallely(univIdSet);
  }

  public void captureUniv(int univId) throws IOException,
    InterruptedException
  {
    Document doc = getUnivDoc(univId);
    String name = getUnivNameFromDoc(doc);

    Collection<String> userIds = getUserIds(doc);

    System.out.println("Obtained " + userIds.size() + " uncaptured users for college: " + name);

    captureUsers(userIds);

    mapper.setCollegeStatus(name, univId, true);

    System.out.println("Updated status as done for: " + name);
  }

  private Document getUnivDoc(int univId) throws IOException
  {
    String univUrl = EDULIX_UNI_COMMON_URL + univId;
    return Jsoup.connect(univUrl).get();
  }

  private void captureUsersParallely(Set<Integer> univIds) throws InterruptedException, IOException
  {
    List<String> exhaustiveUserIds = new ArrayList<String>();
    for (int univId : univIds)
    {
      exhaustiveUserIds.addAll(getUserIds(univId));
    }

    System.out.println("Distributing " + exhaustiveUserIds.size() + " jobs to " + MAX_THREADS + " threads");

    List<List<String>> partition = Lists.partition(exhaustiveUserIds, PARTITION_SIZE);

    new Parallel<List<String>>()
    {
      @Override protected void executeOnItem(List<String> userIds) throws IOException,
        InterruptedException
      {
        captureUsers(userIds);
      }
    }.doParallelOp(partition);
  }


  private void captureUnivsParallely(Set<Integer> univIds) throws InterruptedException
  {
    new Parallel<Integer>()
    {
      @Override protected void executeOnItem(Integer univId) throws IOException,
        InterruptedException
      {
        captureUniv(univId);
      }
    }.doParallelOp(univIds);
  }

  private void captureUsers(Collection<String> userIds) throws IOException, InterruptedException
  {
    for (String userId : userIds)
    {
      System.out.println(
        "Started capture for user " + userId + " by " + Thread.currentThread().getName());
      getData(EDULIX_PROFILE_SEARCH_URL + userId);
      Thread.sleep(1);
    }
  }

  public Collection<String> getUserIds(int univId) throws IOException
  {
    return getUserIds(getUnivDoc(univId));
  }

  public Collection<String> getUserIds(Document doc) throws IOException
  {
    Collection<String> userIds = new HashSet<String>();

    Elements elements = doc.select("table a");
    for (Element element : elements)
    {
      String href = element.attributes().get("href").trim();
      String[] split = href.split("=");

      if (split.length != 2)
        continue;

      userIds.add(split[1]);
    }

    List<String> existingUserIds = mapper.getExistingUserIds();

    return subtract(userIds, existingUserIds);
  }

  public void getData(String url) throws IOException
  {
    int collIndex = -1;
    String userId = url.split("=")[1];
    Document doc = Jsoup.connect(url).get();

    List<String> infoList = new ArrayList<String>();
    HashMap<String, String> attrMap = new HashMap<String, String>();
    HashMap<String, String> collMap = new HashMap<String, String>();


    doc.select(":containsOwn(\u00a0)").remove();

    Elements tables = doc.select("table");
    for (Element table : tables)
    {
      for (Element tr : table.select("tr"))
      {
        Elements tds = tr.select("td");

        if(tds.select("table").size() == 0)
        {
          if(tds.size() % 2 != 0 || (tds.size() != 0 && discardPattern.matcher(tds.get(0).text()).find()))
          {
            Element removedEle = tds.remove(0);
            if(removedEle.text().equals(COLLEGE_INDICATOR))
            {
              collIndex = infoList.size();
            }
          }

          for (Element element : tds)
            infoList.add(element.text().trim());
        }
      }
    }

    setAttrMapAndCollFromList(infoList, collIndex, attrMap, collMap);
    consume(alteredKeysMap(attrMap), collMap, url, userId);
  }

  private void consume(Map<String, String> attrMap, HashMap<String, String> collMap, String url, String userId)
  {
    Profile profile = new Profile();

    int quant = getIntIfPresent(attrMap, profile, "quant");
    int verbal = getIntIfPresent(attrMap, profile, "verbal");

    profile.setQuant(quant);
    profile.setVerbal(verbal);
    profile.setGre(quant + verbal);
    profile.setAwe(getFloatIfPresent(attrMap, profile, "awa"));
    profile.setToefl(getIntIfPresent(attrMap, profile, "toefl"));
    profile.setUsername(userId);
    profile.setName(attrMap.get("name"));
    profile.setProgram(attrMap.get("program"));
    profile.setMajor(attrMap.get("major"));
    profile.setSpecialization(attrMap.get("specialization"));
    profile.setTerm(attrMap.get("term"));
    profile.setUniversity(attrMap.get("university"));
    profile.setDept(attrMap.get("dept"));
    profile.setGrade(attrMap.get("grade"));
    profile.setGrade_scale(attrMap.get("grade_scale"));
    profile.setOthers(attrMap.get("toppergrade"));
    profile.setLink(url);

//    dbLayer.startTransaction();

    mapper.insertProfile(profile);

    for (String collName : collMap.keySet())
    {
      College college = mapper.getCollege(collName);
      if(college == null)
      {
        college = new College(collName);
        mapper.insertColleges(college);
      }
      ProfileCollMapping pcm = new ProfileCollMapping(profile.getId(), college.getId(), collMap.get(collName));
      mapper.insertProfileCollegeStatus(pcm);
    }

//    dbLayer.commit();
  }

  private int getIntIfPresent(Map<String, String> attrMap, Profile profile, String attr)
  {
    try
    {
      return (attrMap.get(attr) != null) ? Integer.parseInt(attrMap.get(attr)) : -1;
    }
    catch (NumberFormatException e)
    {
      return -1;
    }
  }

  private float getFloatIfPresent(Map<String, String> attrMap, Profile profile, String attr)
  {
    try
    {
      return (attrMap.get(attr) != null)? Float.parseFloat(attrMap.get(attr)) : -1;
    }
    catch (NumberFormatException e)
    {
      return -1;
    }
  }

  private Map<String, String> alteredKeysMap(HashMap<String, String> attrMap)
  {
    Map<String,String> map = new HashMap<String, String>();

    for (String k : attrMap.keySet())
    {
      String key = reMap.get(k);
      String value = attrMap.get(k);
      map.put(key, value);
    }
    return map;
  }


  private void setAttrMapAndCollFromList(List<String> list, int collIndex,
                                                        HashMap<String, String> attrMap,
                                                        Map<String, String> collMap)
  {
    String key = null;
    for (int i = 0; i < list.size(); i++)
    {
      if(i % 2 == 0)
        key = list.get(i);
      else
      {
        if(i < collIndex || collIndex < 0)
          attrMap.put(key, list.get(i));
        else
          collMap.put(key, list.get(i));
      }
    }
  }

  private String getUnivNameFromDoc(Document doc)
  {
    Elements e = doc.select("td b");
    return e.get(0).text().trim();
  }
}
