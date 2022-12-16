package com.sciatta.hummer.core.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by Rain on 2022/12/15<br>
 * All Rights Reserved(C) 2017 - 2022 SCIATTA <br> <p/>
 * GsonUtilsTests
 */
public class GsonUtilsTests {
    @Test
    @SuppressWarnings("unchecked")
    public void testToJson() {
        GsonUtils.register(Animal.class, new Class[]{Dog.class, Cat.class});

        String s = GsonUtils.toJson(create());
        System.out.println(s);

        Zoo zoo = GsonUtils.fromJson(s, Zoo.class);
        System.out.println(zoo);
    }

    private Zoo create() {
        Zoo zoo = new Zoo();
        zoo.setName("LX");

        Dog dog = new Dog("dog", "big bone");
        Cat cat = new Cat("cat", "small fish");

        zoo.getAnimals().add(dog);
        zoo.getAnimals().add(cat);

        return zoo;
    }

    static class Zoo {
        private String name;
        private List<Animal> animals = new ArrayList<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<Animal> getAnimals() {
            return animals;
        }

        public void setAnimals(List<Animal> animals) {
            this.animals = animals;
        }

        @Override
        public String toString() {
            return "Zoo{" +
                    "name='" + name + '\'' +
                    ", animals=" + animals +
                    '}';
        }
    }

    abstract static class Animal {
        protected String type;

        public Animal(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    static class Dog extends Animal {
        private String bone;

        public Dog(String type, String bone) {
            super(type);
            this.bone = bone;
        }

        public String getBone() {
            return bone;
        }

        public void setBone(String bone) {
            this.bone = bone;
        }
    }

    static class Cat extends Animal {
        private String fish;

        public Cat(String type, String fish) {
            super(type);
            this.fish = fish;
        }

        public String getFish() {
            return fish;
        }

        public void setFish(String fish) {
            this.fish = fish;
        }
    }
}
