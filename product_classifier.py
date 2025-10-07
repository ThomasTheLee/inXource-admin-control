"""
Product Classification Module
Maps product names/descriptions to standardized product types for search optimization
"""
import re

class ProductClassifier:
    """
    Handles product type classification using keyword matching and AI fallbacks
    """
    
    # Comprehensive product type mappings
    PRODUCT_MAPPINGS = {
        'phone': [
            'phone', 'iphone', 'pixel', 'samsung', 'galaxy', 'smartphone',
            'mobile', 'android', 'ios', 'huawei', 'oneplus', 'xiaomi',
            'oppo', 'vivo', 'realme', 'motorola', 'nokia'
        ],
        'laptop': [
            'laptop', 'macbook', 'computer', 'notebook', 'chromebook',
            'thinkpad', 'dell', 'hp', 'lenovo', 'asus', 'acer', 'surface'
        ],
        'tablet': [
            'tablet', 'ipad', 'kindle', 'surface pro', 'galaxy tab'
        ],
        'tv': [
            'tv', 'television', 'smart tv', 'oled', 'qled', 'monitor',
            '4k', '8k', 'hdtv', 'screen', 'display'
        ],
        'headphones': [
            'headphone', 'earphone', 'earbud', 'airpod', 'beats',
            'headset', 'earpiece', 'audio'
        ],
        'watch': [
            'watch', 'smartwatch', 'apple watch', 'fitbit', 'garmin',
            'timepiece', 'wristwatch'
        ],
        'camera': [
            'camera', 'dslr', 'mirrorless', 'gopro', 'canon', 'nikon',
            'sony camera', 'webcam'
        ],
        'speaker': [
            'speaker', 'bluetooth speaker', 'soundbar', 'jbl', 'bose',
            'echo', 'home pod'
        ],
        'shoes': [
            'shoe', 'sneaker', 'boot', 'sandal', 'slipper', 'heel',
            'trainer', 'footwear', 'nike', 'adidas', 'puma', 'loafer',
            'flip flop', 'croc'
        ],
        'shirt': [
            'shirt', 't-shirt', 'tshirt', 'tee', 'blouse', 'polo',
            'tank top', 'top'
        ],
        'pants': [
            'pant', 'jean', 'trouser', 'chino', 'slack', 'legging',
            'jogger', 'sweatpant'
        ],
        'dress': [
            'dress', 'gown', 'frock', 'sundress', 'maxi dress'
        ],
        'jacket': [
            'jacket', 'coat', 'blazer', 'hoodie', 'cardigan',
            'windbreaker', 'parka', 'bomber'
        ],
        'sweater': [
            'sweater', 'jumper', 'pullover', 'sweatshirt', 'knit'
        ],
        'bag': [
            'bag', 'backpack', 'purse', 'handbag', 'satchel', 'tote',
            'clutch', 'duffel', 'messenger bag', 'wallet'
        ],
        'jewelry': [
            'jewelry', 'necklace', 'bracelet', 'ring', 'earring',
            'pendant', 'chain', 'anklet', 'brooch'
        ],
        'sunglasses': [
            'sunglass', 'shades', 'eyewear', 'spectacle', 'glass'
        ],
        'perfume': [
            'perfume', 'cologne', 'fragrance', 'scent', 'eau de'
        ],
        'makeup': [
            'makeup', 'cosmetic', 'foundation', 'lipstick', 'mascara',
            'eyeshadow', 'blush', 'concealer'
        ],
        'skincare': [
            'cream', 'lotion', 'serum', 'moisturizer', 'cleanser',
            'toner', 'sunscreen', 'skincare', 'face wash'
        ],
        'soap': [
            'soap', 'body wash', 'shower gel', 'hand wash',
            'shampoo', 'conditioner'
        ],
        'flowers': [
            'flower', 'rose', 'bouquet', 'lily', 'tulip',
            'orchid', 'daisy', 'carnation', 'sunflower'
        ],
        'cake': [
            'cake', 'cupcake', 'pastry', 'dessert', 'torte',
            'sponge cake', 'cheesecake'
        ],
        'bread': [
            'bread', 'bun', 'roll', 'loaf', 'baguette', 'croissant'
        ],
        'coffee': [
            'coffee', 'espresso', 'latte', 'cappuccino', 'mocha',
            'americano'
        ],
        'tea': [
            'tea', 'green tea', 'black tea', 'herbal tea', 'chai'
        ],
        'juice': [
            'juice', 'smoothie', 'beverage', 'nectar'
        ],
        'wine': [
            'wine', 'champagne', 'prosecco', 'red wine', 'white wine'
        ],
        'beer': [
            'beer', 'ale', 'lager', 'stout', 'ipa'
        ],
        'book': [
            'book', 'novel', 'textbook', 'ebook', 'paperback',
            'hardcover', 'magazine', 'journal'
        ],
        'toy': [
            'toy', 'action figure', 'doll', 'puzzle', 'lego',
            'game', 'playset'
        ],
        'furniture': [
            'chair', 'table', 'sofa', 'couch', 'desk',
            'cabinet', 'shelf', 'drawer', 'stool', 'bench', 'furniture'
        ],
        'mattress': [
            'mattress', 'bed frame', 'pillow', 'bedding'
        ],
        'lamp': [
            'lamp', 'bulb', 'chandelier', 'lantern', 'lighting'
        ],
        'cookware': [
            'cookware', 'utensil', 'kitchenware'
        ],
        'blender': [
            'blender', 'mixer', 'juicer', 'grinder', 'processor'
        ],
        'microwave': [
            'microwave', 'oven', 'toaster', 'air fryer'
        ],
        'refrigerator': [
            'refrigerator', 'fridge', 'freezer', 'cooler'
        ],
        'vacuum': [
            'vacuum', 'cleaner', 'hoover', 'sweeper'
        ],
        'air_conditioner': [
            'air conditioner', 'ac unit', 'hvac', 'cooling system'
        ],
        'bicycle': [
            'bicycle', 'bike', 'cycle', 'mountain bike', 'road bike'
        ],
        'car': [
            'car', 'vehicle', 'automobile', 'sedan', 'suv', 'truck'
        ],
        'tire': [
            'tire', 'tyre', 'wheel', 'rim'
        ],
        'battery': [
            'battery', 'power bank'
        ],
        'charger': [
            'charger', 'adapter', 'charging cable'
        ],
        'solar_panel': [
            'solar panel', 'photovoltaic', 'pv panel', 'solar module'
        ],
        'inverter': [
            'inverter', 'power inverter', 'sine wave'
        ],
        'charge_controller': [
            'charge controller', 'solar controller', 'mppt', 'pwm controller'
        ],
        'guitar': [
            'guitar', 'bass', 'ukulele', 'string instrument'
        ],
        'keyboard': [
            'keyboard', 'piano', 'synthesizer'
        ],
        'drum': [
            'drum', 'percussion', 'cymbal', 'snare'
        ],
        'vitamins': [
            'vitamin', 'supplement', 'mineral', 'capsule'
        ],
        'protein': [
            'protein', 'whey', 'supplement powder', 'shake'
        ],
        'diapers': [
            'diaper', 'nappy', 'wipe', 'baby care'
        ],
        'stroller': [
            'stroller', 'pram', 'pushchair', 'baby carrier'
        ],
        'paint': [
            'paint', 'coating'
        ],
        'tool': [
            'hammer', 'screwdriver', 'drill', 'wrench',
            'saw', 'plier', 'toolset'
        ],
        'seeds': [
            'seed', 'seedling', 'fertilizer'
        ],
        'pet_food': [
            'dog food', 'cat food', 'pet food', 'kibble'
        ],
        'collar': [
            'collar', 'leash', 'harness', 'pet accessory'
        ],
        'filter': [
            'filter', 'air filter', 'oil filter', 'water filter'
        ],
        'brake_pad': [
            'brake pad', 'brake shoe', 'brake disc'
        ]
    }
    
    # Category words to avoid (too broad)
    INVALID_CATEGORIES = [
        'retail', 'wholesale', 'telecommunications', 'electronics',
        'food', 'beverage', 'clothing', 'footwear', 'furniture',
        'beauty', 'personal', 'care', 'real', 'estate', 'fashion',
        'apparel', 'accessories', 'home', 'garden', 'sports',
        'automotive', 'health', 'toys', 'books', 'music', 'system',
        'kits', 'brakes', 'filters', 'lighting', 'inverters', 'batteries',
        'controllers', 'panels'
    ]
    
    @classmethod
    def classify_from_text(cls, name, description=""):
        """
        Classify a product based on its name and description using keyword matching
        with word boundary detection to avoid false matches.
        
        Args:
            name (str): Product name
            description (str): Product description
            
        Returns:
            str: Product type or None if no match found
        """
        # Combine name and description for searching
        search_text = f"{name} {description}".lower()
        
        # Sort by keyword length (longest first) to match more specific terms first
        sorted_types = sorted(
            cls.PRODUCT_MAPPINGS.items(),
            key=lambda x: max(len(kw) for kw in x[1]),
            reverse=True
        )
        
        # Check each product type's keywords
        for product_type, keywords in sorted_types:
            for keyword in keywords:
                # Use word boundary regex to match whole words only
                # \b ensures we match complete words, not substrings
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, search_text):
                    print(f"  [CLASSIFIER] Matched keyword '{keyword}' → type '{product_type}'")
                    return product_type
        
        return None
    
    @classmethod
    def is_valid_product_type(cls, product_type):
        """
        Check if a product type is valid (not a broad category)
        
        Args:
            product_type (str): Product type to validate
            
        Returns:
            bool: True if valid, False if it's a broad category
        """
        return product_type.lower() not in cls.INVALID_CATEGORIES
    
    @classmethod
    def get_ai_examples(cls):
        """
        Returns a list of example conversations for AI few-shot learning
        
        Returns:
            list: List of dictionaries with role/content for OpenAI API
        """
        return [
            {
                "role": "user", 
                "content": "Product name: Pixel 9\nDescription: smartphone with camera\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "phone"
            },
            {
                "role": "user", 
                "content": "Product name: iPhone 16\nDescription: Apple smartphone with A18 chip\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "phone"
            },
            {
                "role": "user", 
                "content": "Product name: Nike Air Max\nDescription: athletic sneakers\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "shoes"
            },
            {
                "role": "user", 
                "content": "Product name: MacBook Pro M3\nDescription: Apple laptop computer\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "laptop"
            },
            {
                "role": "user", 
                "content": "Product name: 300W Solar Panel\nDescription: monocrystalline solar panel\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "panel"
            },
            {
                "role": "user", 
                "content": "Product name: 200Ah Battery\nDescription: deep cycle battery for solar\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "battery"
            },
            {
                "role": "user", 
                "content": "Product name: 3000W Inverter\nDescription: pure sine wave power inverter\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "inverter"
            },
            {
                "role": "user", 
                "content": "Product name: Red roses 20 stems\nDescription: bouquet of roses\n\nIn one word, what specific product is this?"
            },
            {
                "role": "assistant", 
                "content": "flowers"
            }
        ]
    
    @classmethod
    def get_all_product_types(cls):
        """
        Returns a list of all available product types
        
        Returns:
            list: Sorted list of product type names
        """
        return sorted(cls.PRODUCT_MAPPINGS.keys())